package org.borer.logdb.bbtree;

import org.borer.logdb.Config;
import org.borer.logdb.storage.NodesManager;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class BTree
{
    private static final int THRESHOLD_CHILDREN_PER_NODE = 4;
    /**
     * This designates the "last stored" version for a store which was
     * just open for the first time.
     */
    private static final long INITIAL_VERSION = 0;

    /**
     * Reference to the current uncommitted root page.
     */
    private final AtomicReference<RootReference> uncommittedRoot;
    /**
     * Reference to the current committed root page number.
     */
    private final AtomicReference<Long> committedRoot;

    private final NodesManager nodesManager;

    private long nodesCount;

    private long writeVersion;

    public BTree(final NodesManager nodesManager)
    {
        this.nodesManager = Objects.requireNonNull(
                nodesManager, "nodesManager must not be null");
        final long lastRootPageNumber = nodesManager.loadLastRootPageNumber();
        this.committedRoot = new AtomicReference<>(lastRootPageNumber);

        final boolean isNewBtree = lastRootPageNumber == -1;
        if (isNewBtree)
        {
            final RootReference rootReference = new RootReference(
                    nodesManager.createEmptyLeafNode(),
                    System.currentTimeMillis(),
                    INITIAL_VERSION,
                    null);
            this.uncommittedRoot = new AtomicReference<>(rootReference);
            this.writeVersion = INITIAL_VERSION;
        }
        else
        {
            this.uncommittedRoot = new AtomicReference<>(null);
            final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
            mappedNode.initNode(lastRootPageNumber);
            this.writeVersion = mappedNode.getVersion();
        }

        this.nodesCount = 1;
    }

    /**
     * Remove a key and the associated value, if the key exists.
     *
     * @param key the key (may not be null)
     */
    public void remove(final long key)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

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

                final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode);
                updatePathToRoot(parentCursor, targetNode);
                return;
            }
            assert currentNode.getKeyCount() > 1;
        }

        final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode);
        targetNode.remove(index);

        nodesManager.returnMappedNode(mappedNode);

        updatePathToRoot(parentCursor, targetNode);
    }

    /**
     * Inserts a new key/value into the BTree. Grows the tree if needed.
     * If the key already exists, overrides the value for it
     *
     * @param key   the kay to insert/set
     * @param value the value
     */
    public void put(final long key, final long value)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        BTreeNode targetNode = cursorPosition.getNode(mappedNode);
        CursorPosition parentCursor = cursorPosition.parent;

        BTreeNodeHeap currentNode = nodesManager.copyNode(targetNode);
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > Config.MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = currentNode.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(currentNode, at);

            if (parentCursor == null)
            {
                this.nodesCount++;
                final BTreeNodeHeap temp = nodesManager.createEmptyNonLeafNode();

                temp.insertChild(0, keyAt, currentNode);
                temp.setChild(1, split);

                currentNode = temp;

                break;
            }

            final BTreeNodeHeap parentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode));
            parentNode.setChild(parentCursor.index, split);
            parentNode.insertChild(parentCursor.index, keyAt, currentNode);

            parentCursor = parentCursor.parent;
            currentNode = parentNode;
            keyCount = currentNode.getKeyCount();
        }

        nodesManager.returnMappedNode(mappedNode);

        updatePathToRoot(parentCursor, currentNode);
    }

    /**
     * Gets a value for the key at time/instance t.
     *
     * @param key     the key to search for
     * @param version the version that we are interested. Must be >= 0
     */
    public long get(final long key, final int version)
    {
        assert version >= 0;

        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = getLastCursorPosition(key, version);
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final BTreeNode node = cursorPosition.getNode(mappedNode);
        if (node == null)
        {
            throw new IllegalArgumentException("Didn't have version " + version);
        }

        final long value = node.get(key);
        nodesManager.returnMappedNode(mappedNode);

        return value;
    }

    public long get(final long key)
    {
        //TODO optimize, we don't need the whole path, just the end node.
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final long value = cursorPosition.getNode(mappedNode).get(key);
        nodesManager.returnMappedNode(mappedNode);
        return value;
    }

    private CursorPosition getLastCursorPosition(long key)
    {
        CursorPosition cursorPosition;
        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null)
        {
            cursorPosition = traverseDown(rootReference.root, key);
        }
        else
        {
            cursorPosition = traverseDown(committedRoot.get(), key);
        }
        return cursorPosition;
    }

    private CursorPosition getLastCursorPosition(long key, int version)
    {
        final CursorPosition cursorPosition;
        final RootReference currentRootReference = uncommittedRoot.get();
        if (currentRootReference != null)
        {
            final RootReference rootNodeForVersion = currentRootReference.getRootReferenceForVersion(version);
            if (rootNodeForVersion == null)
            {
                cursorPosition = traverseDown(committedRoot.get(), key);
            }
            else
            {
                cursorPosition = traverseDown(rootNodeForVersion.root, key);
            }
        }
        else
        {
            cursorPosition = traverseDown(committedRoot.get(), key);
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
            final Long committedRootPageNumber = committedRoot.get();
            final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
            mappedNode.initNode(committedRootPageNumber);
            final boolean isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
            nodesManager.returnMappedNode(mappedNode);
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
    public void consumeAll(final int version, final BiConsumer<Long, Long> consumer)
    {
        assert version >= 0;

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
                long committedRootPageNumber = committedRoot.get();
                final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
                mappedNode.initNode(committedRootPageNumber);
                while (mappedNode.getVersion() > version)
                {
                    committedRootPageNumber = mappedNode.getPreviousRoot();
                    mappedNode.initNode(committedRootPageNumber);
                }

                final boolean isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
                nodesManager.returnMappedNode(mappedNode);
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
            long committedRootPageNumber = committedRoot.get();
            final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
            mappedNode.initNode(committedRootPageNumber);
            while (mappedNode.getVersion() > version)
            {
                committedRootPageNumber = mappedNode.getPreviousRoot();
                mappedNode.initNode(committedRootPageNumber);
            }

            final boolean isNonLeaf = mappedNode.getNodeType() == BtreeNodeType.NonLeaf;
            nodesManager.returnMappedNode(mappedNode);
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

    public void commit()
    {
        nodesManager.commitDirtyNodes();

        final RootReference uncommittedRootReference = uncommittedRoot.get();
        if (uncommittedRootReference != null)
        {
            final long pageNumber = uncommittedRootReference.getPageNumber();
            nodesManager.commitLastRootPage(pageNumber);

            uncommittedRoot.set(null);
            committedRoot.set(pageNumber);
        }
    }

    public void close()
    {
        nodesManager.close();
    }

    public String print()
    {
        return BTreePrinter.print(this, nodesManager);
    }

    private void consumeNonLeafNode(final BiConsumer<Long, Long> consumer, final long nonLeafPageNumber)
    {
        assert nonLeafPageNumber > 0;

        final BTreeMappedNode nonLeafNode = nodesManager.getOrCreateMappedNode();
        final BTreeMappedNode childNode = nodesManager.getOrCreateMappedNode();

        nonLeafNode.initNode(nonLeafPageNumber);
        final int childPageCount = nonLeafNode.getChildrenNumber();
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

        nodesManager.returnMappedNode(childNode);
        nodesManager.returnMappedNode(nonLeafNode);
    }

    private void consumeNonLeafNode(final BiConsumer<Long, Long> consumer, final BTreeNode nonLeaf)
    {
        assert nonLeaf != null;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        final int childPageCount = nonLeaf.getChildrenNumber();
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

        nodesManager.returnMappedNode(mappedNode);
    }

    private void consumeLeafNode(final BiConsumer<Long, Long> consumer, final long leafPageNumber)
    {
        assert leafPageNumber > 0;

        final BTreeMappedNode mappedLeaf = nodesManager.getOrCreateMappedNode();
        mappedLeaf.initNode(leafPageNumber);

        final int keyCount = mappedLeaf.getKeyCount();
        for (int i = 0; i < keyCount; i++)
        {
            final long key = mappedLeaf.getKey(i);
            final long value = mappedLeaf.getValue(i);

            consumer.accept(key, value);
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

    private CursorPosition traverseDown(final BTreeNode root, final long key)
    {
        BTreeNode node = root;
        CursorPosition cursor = null;
        int index;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        while (node.getNodeType() == BtreeNodeType.NonLeaf)
        {
            assert node.getKeyCount() > 0
                    : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getKeyCount());
            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
            node = nodesManager.loadNode(index, node, mappedNode);
        }

        index = node.getKeyIndex(key);
        cursor = createCursorPosition(node, index, cursor);
        nodesManager.returnMappedNode(mappedNode);

        return cursor;
    }

    private CursorPosition traverseDown(final long rootPageNumber, final long key)
    {
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        mappedNode.initNode(rootPageNumber);

        BTreeNode node = mappedNode;
        CursorPosition cursor = null;
        int index;

        while (node.getNodeType() == BtreeNodeType.NonLeaf)
        {
            assert node.getKeyCount() > 0
                    : String.format("non leaf node should always have at least 1 key. Current node had %d", node.getKeyCount());
            index = node.getKeyIndex(key);
            cursor = createCursorPosition(node, index, cursor);
            node = nodesManager.loadNode(index, node, mappedNode);
        }

        index = node.getKeyIndex(key);
        cursor = createCursorPosition(node, index, cursor);
        nodesManager.returnMappedNode(mappedNode);

        return cursor;
    }

    private CursorPosition createCursorPosition(
            final BTreeNode node,
            final int index,
            final CursorPosition parentCursor)
    {
        if (node instanceof BTreeMappedNode)
        {
            return new CursorPosition(null, node.getPageNumber(), index, parentCursor);
        }
        else
        {
            return new CursorPosition(node, -1, index, parentCursor);
        }
    }

    private void updatePathToRoot(final CursorPosition cursor, final BTreeNodeHeap current)
    {
        BTreeNodeHeap currentNode = current;
        CursorPosition parentCursor = cursor;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        while (parentCursor != null)
        {
            BTreeNodeHeap c = currentNode;
            currentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode));
            currentNode.setChild(parentCursor.index, c);
            parentCursor = parentCursor.parent;
        }
        nodesManager.returnMappedNode(mappedNode);

        setNewRoot(currentNode);
    }

    BTreeNode getCurrentUncommittedRootNode()
    {
        final RootReference rootReference = uncommittedRoot.get();
        return rootReference == null ? null : rootReference.root;
    }

    long getCurrentCommittedRootNode()
    {
        final long committedRootPageNumber = committedRoot.get();
        assert committedRootPageNumber > 0 : "Committed root page number must be positive. Current " + committedRootPageNumber;
        return committedRootPageNumber;
    }

    /**
     * Try to set the new uncommittedRoot reference from now on.
     *
     * @param newRootPage the new uncommittedRoot page
     * @return new RootReference or null if update failed
     */
    private RootReference setNewRoot(final BTreeNode newRootPage)
    {
        Objects.requireNonNull(newRootPage, "current uncommittedRoot cannot be null");
        final RootReference currentRoot = uncommittedRoot.get();

        final long newVersion = writeVersion++;

        //TODO: extract timestamp retriever
        final long timestamp = System.currentTimeMillis();
        final RootReference updatedRootReference = new RootReference(newRootPage, timestamp, newVersion, currentRoot);
        boolean success = uncommittedRoot.compareAndSet(currentRoot, updatedRootReference);

        if (success)
        {
            nodesManager.addDirtyRoot(updatedRootReference);
            return updatedRootReference;
        }
        else
        {
            return null;
        }
    }

    public long getNodesCount()
    {
        return this.nodesCount;
    }
}
