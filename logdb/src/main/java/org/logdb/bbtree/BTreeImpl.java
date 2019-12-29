package org.logdb.bbtree;

import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.util.function.BiConsumer;

public class BTreeImpl extends BTreeAbstract
{
    private static final @ByteSize int REQUIRED_SPACE = StorageUnits.size(2 * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));

    public BTreeImpl(
            final NodesManager nodesManager,
            final TimeSource timeSource,
            final @Version long nextWriteVersion,
            final @PageNumber long lastRootPageNumber,
            final RootReference rootReference)
    {
        super(nodesManager, timeSource, nextWriteVersion, lastRootPageNumber, rootReference);
    }

    /**
     * Remove a key and the associated value, if the key exists.
     *
     * @param key the key (may not be null)
     */
    @Override
    public void remove(final byte[] key)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final @Version long newVersion = nextWriteVersion++;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            int index = cursorPosition.index;
            BTreeNode currentNode = cursorPosition.getNode(mappedNode);
            CursorPosition parentCursor = cursorPosition.parent;

            while (currentNode.getPairCount() == 1 && parentCursor != null)
            {
                this.nodesCount--;
                index = parentCursor.index;
                currentNode = parentCursor.getNode(mappedNode);
                parentCursor = parentCursor.parent;
            }

            final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode, newVersion);
            targetNode.removeAtIndex(index);
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
    public void put(final byte[] key, final byte[] value)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final @Version long newVersion = nextWriteVersion++;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            BTreeNode targetNode = cursorPosition.getNode(mappedNode);
            CursorPosition parentCursor = cursorPosition.parent;

            BTreeNodeHeap currentNode = nodesManager.copyNode(targetNode, newVersion);
            currentNode.insert(key, value);

            while (currentNode.shouldSplit(REQUIRED_SPACE))
            {
                this.nodesCount++;
                int keyCount = currentNode.getPairCount();
                final int at = keyCount >> 1;
                final byte[] keyAt = currentNode.getKey(at);
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
    public byte[] get(final byte[] key, final @Version long version)
    {
        assert version >= 0;

        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode rootNode = getRootNode(version, mappedNode);
            final CursorPosition cursorPosition = traverseDown(rootNode, key);
            final BTreeNode node = cursorPosition.getNode(mappedNode);

            return node.get(key);
        }
    }

    @Override
    public byte[] get(final byte[] key)
    {
        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            return cursorPosition.getNode(mappedNode).get(key);
        }
    }

    @Override
    public byte[] getByTimestamp(final byte[] key, final @Milliseconds long timestamp)
    {
        assert timestamp >= 0;

        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode rootNode = getRootNodeByTimestamp(timestamp, mappedNode);
            final CursorPosition cursorPosition = traverseDown(rootNode, key);
            final BTreeNode node = cursorPosition.getNode(mappedNode);

            return node.get(key);
        }
    }

    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final BiConsumer<byte[], byte[]> consumer)
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
            boolean isNonLeaf;
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

    //TODO: this is going to be replaced with snapshot interface
    /**
     * Calls the consumer for all the key/value pairs in linear scan, from start to end.
     * @param version Version that we want to scan for
     * @param consumer A consumer that will accept the key/value pairs
     */
    public void consumeAll(final @Version long version, final BiConsumer<byte[], byte[]> consumer)
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
                boolean isNonLeaf;
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
            boolean isNonLeaf;
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

    private void consumeNonLeafNode(final BiConsumer<byte[], byte[]> consumer, final @PageNumber long nonLeafPageNumber)
    {
        assert nonLeafPageNumber > 0;

        try (BTreeMappedNode  nonLeafNode = nodesManager.getOrCreateMappedNode())
        {
            try (BTreeMappedNode  childNode = nodesManager.getOrCreateMappedNode())
            {
                nonLeafNode.initNode(nonLeafPageNumber);
                final int childPageCount = nonLeafNode.getPairCount();
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

    private void consumeNonLeafNode(final BiConsumer<byte[], byte[]> consumer, final BTreeNode nonLeaf)
    {
        assert nonLeaf != null;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final int childPageCount = nonLeaf.getPairCount();
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

    private void consumeLeafNode(final BiConsumer<byte[], byte[]> consumer, final @PageNumber long leafPageNumber)
    {
        assert leafPageNumber > 0;

        try (BTreeMappedNode  mappedLeaf = nodesManager.getOrCreateMappedNode())
        {
            mappedLeaf.initNode(leafPageNumber);

            final int keyCount = mappedLeaf.getPairCount();
            for (int i = 0; i < keyCount; i++)
            {
                final byte[] key = mappedLeaf.getKey(i);
                final byte[] value = mappedLeaf.getValue(i);

                consumer.accept(key, value);
            }
        }
    }

    private void consumeLeafNode(final BiConsumer<byte[], byte[]> consumer, final BTreeNode leaf)
    {
        assert leaf != null;

        final int keyCount = leaf.getPairCount();
        for (int i = 0; i < keyCount; i++)
        {
            final byte[] key = leaf.getKey(i);
            final byte[] value = leaf.getValue(i);

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
