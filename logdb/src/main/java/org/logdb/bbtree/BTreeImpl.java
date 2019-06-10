package org.logdb.bbtree;

import org.logdb.storage.NodesManager;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.TimeSource;

import java.util.function.BiConsumer;

public class BTreeImpl extends BTreeAbstract
{
    public BTreeImpl(final NodesManager nodesManager, final TimeSource timeSource)
    {
        super(nodesManager, timeSource);
    }

    /**
     * Remove a key and the associated value, if the key exists.
     *
     * @param key the key (may not be null)
     */
    @Override
    public void remove(final long key)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final @Version long newVersion = writeVersion++;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            int index = cursorPosition.index;
            BTreeNode currentNode = cursorPosition.getNode(mappedNode);
            CursorPosition parentCursor = cursorPosition.parent;

            if (currentNode.getKeyCount() == 1 && parentCursor != null)
            {
                this.nodesCount--;
                index = parentCursor.index;
                currentNode = parentCursor.getNode(mappedNode);
                parentCursor = parentCursor.parent;

                if (currentNode.getKeyCount() == 1)
                {
                    assert currentNode.getNodeType() == BtreeNodeType.NonLeaf
                            : "Parent of the node that trying to remove is NOT non leaf";

                    this.nodesCount--;
                    assert index <= 1;
                    currentNode = nodesManager.loadNode(1 - index, currentNode, mappedNode);

                    final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode, newVersion);
                    updatePathToRoot(parentCursor, targetNode);
                    return;
                }
                assert currentNode.getKeyCount() > 1;
            }

            final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode, newVersion);
            targetNode.remove(index);
            updatePathToRoot(parentCursor, targetNode);
        }
    }

    /**
     * Inserts a new key/value into the BTree. Grows the tree if needed.
     * If the key already exists, overrides the value for it
     *
     * @param key   the kay to insert/set
     * @param value the value
     */
    @Override
    public void put(final long key, final long value)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final @Version long newVersion = writeVersion++;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {

            BTreeNode targetNode = cursorPosition.getNode(mappedNode);
            CursorPosition parentCursor = cursorPosition.parent;

            BTreeNodeHeap currentNode = nodesManager.copyNode(targetNode, newVersion);
            currentNode.insert(key, value);

            while (currentNode.shouldSplit())
            {
                this.nodesCount++;
                int keyCount = currentNode.getKeyCount();
                final int at = keyCount >> 1;
                final long keyAt = currentNode.getKey(at);
                final BTreeNodeHeap split = nodesManager.splitNode(currentNode, at, newVersion);

                if (parentCursor == null)
                {
                    this.nodesCount++;
                    final BTreeNodeHeap temp = nodesManager.createEmptyNonLeafNode();
                    temp.setVersion(newVersion);

                    temp.insertChild(0, keyAt, currentNode);
                    temp.setChild(1, split);

                    currentNode = temp;

                    break;
                }

                final BTreeNodeHeap parentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode), newVersion);
                parentNode.setChild(parentCursor.index, split);
                parentNode.insertChild(parentCursor.index, keyAt, currentNode);

                parentCursor = parentCursor.parent;
                currentNode = parentNode;
                keyCount = currentNode.getKeyCount();
            }

            updatePathToRoot(parentCursor, currentNode);
        }
    }

    /**
     * Gets a value for the key at time/instance t.
     *
     * @param key     the key to search for
     * @param version the version that we are interested. Must be &gt;= 0
     */
    @Override
    public long get(final long key, final @Version long version)
    {
        assert version >= 0;

        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = getLastCursorPosition(key, version);
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode node = cursorPosition.getNode(mappedNode);
            if (node == null)
            {
                throw new IllegalArgumentException("Didn't have version " + version);
            }

            return node.get(key);
        }
    }

    @Override
    public long get(final long key)
    {
        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            return cursorPosition.getNode(mappedNode).get(key);
        }
    }

    private CursorPosition getLastCursorPosition(final long key, final @Version long version)
    {
        final CursorPosition cursorPosition;
        final RootReference currentRootReference = uncommittedRoot.get();
        if (currentRootReference != null)
        {
            final RootReference rootNodeForVersion = currentRootReference.getRootReferenceForVersion(version);
            if (rootNodeForVersion == null)
            {
                final @PageNumber long committedRootPageNumber = StorageUnits.pageNumber(committedRoot.get());
                if (committedRootPageNumber >= 0)
                {
                    cursorPosition = traverseDown(committedRootPageNumber, key);
                }
                else
                {
                    throw new IllegalArgumentException("Didn't have version " + version);
                }
            }
            else
            {
                cursorPosition = traverseDown(rootNodeForVersion.root, key);
            }
        }
        else
        {
            cursorPosition = traverseDown(StorageUnits.pageNumber(committedRoot.get()), key);
        }

        return cursorPosition;
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final BiConsumer<Long, Long> consumer)
    {
        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null)
        {
            final BTreeNode root = rootReference.root;
            if (root.getNodeType() == BtreeNodeType.NonLeaf)
            {
                consumeNonLeafNode(consumer, root);
            }
            else
            {
                consumeLeafNode(consumer, root);
            }
        }
        else
        {
            final @PageNumber long committedRootPageNumber = StorageUnits.pageNumber(committedRoot.get());
            boolean isNonLeaf = false;
            try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
            {
                mappedNode.initNode(committedRootPageNumber);
                isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
            }

            if (isNonLeaf)
            {
                consumeNonLeafNode(consumer, committedRootPageNumber);
            }
            else
            {
                consumeLeafNode(consumer, committedRootPageNumber);
            }
        }
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param version Version that we want to scan for
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final @Version long version, final BiConsumer<Long, Long> consumer)
    {
        assert version >= 0 : "version must be positive. Provided " + version;

        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null)
        {
            final RootReference rootNodeForVersion = rootReference.getRootReferenceForVersion(version);
            if (rootNodeForVersion != null)
            {
                if (rootNodeForVersion.root.getNodeType() == BtreeNodeType.NonLeaf)
                {
                    consumeNonLeafNode(consumer, rootNodeForVersion.root);
                }
                else
                {
                    consumeLeafNode(consumer, rootNodeForVersion.root);
                }
            }
            else
            {
                @PageNumber long committedRootPageNumber = StorageUnits.pageNumber(committedRoot.get());
                boolean isNonLeaf = false;
                try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
                {
                    mappedNode.initNode(committedRootPageNumber);

                    while (mappedNode.getVersion() > version)
                    {
                        committedRootPageNumber = mappedNode.getPreviousRoot();
                        mappedNode.initNode(committedRootPageNumber);
                    }

                    isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
                }
                if (isNonLeaf)
                {
                    consumeNonLeafNode(consumer, committedRootPageNumber);
                }
                else
                {
                    consumeLeafNode(consumer, committedRootPageNumber);
                }
            }
        }
        else
        {
            @PageNumber long committedRootPageNumber = StorageUnits.pageNumber(committedRoot.get());
            boolean isNonLeaf = false;
            try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
            {
                mappedNode.initNode(committedRootPageNumber);

                while (mappedNode.getVersion() > version)
                {
                    committedRootPageNumber = mappedNode.getPreviousRoot();
                    mappedNode.initNode(committedRootPageNumber);
                }

                isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
            }

            if (isNonLeaf)
            {
                consumeNonLeafNode(consumer, committedRootPageNumber);
            }
            else
            {
                consumeLeafNode(consumer, committedRootPageNumber);
            }
        }
    }

    private void consumeNonLeafNode(final BiConsumer<Long, Long> consumer, final @PageNumber long nonLeafPageNumber)
    {
        assert nonLeafPageNumber > 0;

        try (BTreeMappedNode  nonLeafNode = nodesManager.getOrCreateMappedNode())
        {
            try (BTreeMappedNode  childNode = nodesManager.getOrCreateMappedNode())
            {
                nonLeafNode.initNode(nonLeafPageNumber);
                final int childPageCount = nonLeafNode.getNumberOfChildren();
                for (int i = 0; i < childPageCount; i++)
                {
                    final BTreeNode childPage = nodesManager.loadNode(i, nonLeafNode, childNode);
                    if (childPage.getNodeType() == BtreeNodeType.NonLeaf)
                    {
                        consumeNonLeafNode(consumer, childPage.getPageNumber());
                    }
                    else
                    {
                        consumeLeafNode(consumer, childPage.getPageNumber());
                    }
                }
            }
        }
    }

    private void consumeNonLeafNode(final BiConsumer<Long, Long> consumer, final BTreeNode nonLeaf)
    {
        assert nonLeaf != null;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final int childPageCount = nonLeaf.getNumberOfChildren();
            for (int i = 0; i < childPageCount; i++)
            {
                final BTreeNode childPage = nodesManager.loadNode(i, nonLeaf, mappedNode);
                if (childPage.getNodeType() == BtreeNodeType.NonLeaf)
                {
                    consumeNonLeafNode(consumer, childPage);
                }
                else
                {
                    consumeLeafNode(consumer, childPage);
                }
            }
        }
    }

    private void consumeLeafNode(final BiConsumer<Long, Long> consumer, final @PageNumber long leafPageNumber)
    {
        assert leafPageNumber > 0;

        try (BTreeMappedNode  mappedLeaf = nodesManager.getOrCreateMappedNode())
        {
            mappedLeaf.initNode(leafPageNumber);

            final int keyCount = mappedLeaf.getKeyCount();
            for (int i = 0; i < keyCount; i++)
            {
                final long key = mappedLeaf.getKey(i);
                final long value = mappedLeaf.getValue(i);

                consumer.accept(key, value);
            }
        }
    }

    private void consumeLeafNode(final BiConsumer<Long, Long> consumer, final BTreeNode leaf)
    {
        assert leaf != null;

        final int keyCount = leaf.getKeyCount();
        for (int i = 0; i < keyCount; i++)
        {
            final long key = leaf.getKey(i);
            final long value = leaf.getValue(i);

            consumer.accept(key, value);
        }
    }

    private void updatePathToRoot(final CursorPosition cursor, final BTreeNodeHeap current)
    {
        BTreeNodeHeap currentNode = current;
        CursorPosition parentCursor = cursor;
        final @Version long version = current.getVersion();

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            while (parentCursor != null)
            {
                BTreeNodeHeap c = currentNode;
                currentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode), version);
                currentNode.setChild(parentCursor.index, c);
                parentCursor = parentCursor.parent;
            }
        }

        setNewRoot(currentNode);
    }
}
