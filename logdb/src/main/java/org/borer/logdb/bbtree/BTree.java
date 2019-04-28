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
        final long newVersion = writeVersion++;

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

                final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode, newVersion);
                updatePathToRoot(parentCursor, targetNode);
                return;
            }
            assert currentNode.getKeyCount() > 1;
        }

        final BTreeNodeHeap targetNode = nodesManager.copyNode(currentNode, newVersion);
        targetNode.remove(index);

        nodesManager.returnMappedNode(mappedNode);

        updatePathToRoot(parentCursor, targetNode);
    }

    /**
     * Removes the key from the tree by appending a thombstone without touching leafs.
     * When the tree is spilled, the entry will be actually removed from the leaf.
     * @param key the key that identifies the pair to remove.
     */
    public void removeWithLog(final long key)
    {
        BTreeNode currentNode;
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final long newVersion = writeVersion++;

        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null && rootReference.root != null)
        {
            currentNode = rootReference.root;
        }
        else
        {
            mappedNode.initNode(committedRoot.get());
            currentNode = mappedNode;
        }
        BTreeNodeHeap newRoot = nodesManager.copyNode(currentNode, newVersion);

        removeWithLogInternal(null, -1, newRoot, key);

        if (newRoot.getNodeType() == BtreeNodeType.NonLeaf && newRoot.getKeyCount() == 1)
        {
            this.nodesCount = this.nodesCount - 2;
            final int keyIndex = newRoot.getKeyIndex(key);
            final BTreeNode nodeToRemoveFrom = nodesManager.loadNode(keyIndex, newRoot, mappedNode);
            if (nodeToRemoveFrom.getNodeType() == BtreeNodeType.Leaf && nodeToRemoveFrom.getKeyCount() == 0)
            {
                newRoot = getOrCreateChildrenCopy((BTreeNodeNonLeaf) newRoot, 1 - keyIndex);
            }
        }

        nodesManager.returnMappedNode(mappedNode);

        setNewRoot(newRoot);
    }

    private void removeWithLogInternal(
            final BTreeNodeHeap parent,
            final int nodeIndexInParent,
            final BTreeNodeHeap node,
            final long key)
    {
        if (node.getNodeType() == BtreeNodeType.Leaf)
        {
            final int keyIndex = node.getKeyIndex(key);
            if (keyIndex >= 0)
            {
                node.remove(keyIndex);
            }
        }
        else
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            if (nonLeaf.logHasFreeSpace())
            {
                nonLeaf.insertLog(key, -1);
            }
            else
            {
                final int keyIndex = nonLeaf.getKeyIndex(key);
                final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(nonLeaf, keyIndex);
                nonLeaf.setChild(keyIndex, childrenCopy);
                removeWithLogInternal(nonLeaf, keyIndex, childrenCopy, key);

                //remove log if there is any, as we already removed the key/value in the previous step
                nonLeaf.removeLog(key);

                //spill the rest of the log
                final long[] keyValues = nonLeaf.spillLog();
                assert keyValues.length % 2 == 0 : "log key/value array must even size. Current size " + keyValues.length;
                final int maxIndex = keyValues.length / 2;

                for (int i = 0; i < maxIndex; i++)
                {
                    final int index = i * 2;
                    final long key2 = keyValues[index];
                    final long value2 = keyValues[index + 1];
                    final int keyIndex2 = nonLeaf.getKeyIndex(key2);
                    final BTreeNodeHeap childrenCopy2 = getOrCreateChildrenCopy(nonLeaf, keyIndex2);
                    nonLeaf.setChild(keyIndex2, childrenCopy2);
                    if (value2 < 0)
                    {
                        removeWithLogInternal(nonLeaf, keyIndex2, childrenCopy2, key2);
                    }
                    else
                    {
                        putWithLogInternal(nonLeaf, keyIndex2, childrenCopy2, key2, value2);
                    }
                }

                final BTreeNodeHeap childToRemove = getOrCreateChildrenCopy(nonLeaf, keyIndex);
                if (childToRemove.getKeyCount() == 0 && nonLeaf.getKeyCount() > 1)
                {
                    this.nodesCount--;
                    nonLeaf.remove(keyIndex);
                }
                else if (childToRemove.getKeyCount() == 0 && nonLeaf.getKeyCount() == 1 && parent != null)
                {
                    this.nodesCount = this.nodesCount - 2;
                    final BTreeNodeHeap childrenCopy3 = getOrCreateChildrenCopy(nonLeaf, 1 - keyIndex);
                    parent.setChild(nodeIndexInParent, childrenCopy3);
                }
            }
        }
    }

    /**
     * Removes the key by first walking the tree and searching if it actually has a value with that key.
     * If no value is found this operation is a no-op.
     * @param key the key that identifies the pair to remove.
     */
    public void removeWithLogWithoutFalsePositives(final long key)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final long newVersion = writeVersion + 1;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        int index = cursorPosition.index;
        BTreeNode newRoot = cursorPosition.getNode(mappedNode);
        CursorPosition parentCursor = cursorPosition.parent;
        boolean wasFound = false;

        if (index >= 0 && newRoot.getNodeType() == BtreeNodeType.Leaf)
        {
            wasFound = true;
            boolean isSafeToRemoveParent = false;
            BTreeNode parent = null;
            if (parentCursor != null)
            {
                parent = parentCursor.getNode(mappedNode);
                assert parent.getNodeType() == BtreeNodeType.NonLeaf;
                final int logKeyValuesCount = ((BTreeNodeAbstract) parent).getLogKeyValuesCount();
                isSafeToRemoveParent = logKeyValuesCount == 0;
            }

            //if current leaf has only one element, try to remove directly from parent
            if (newRoot.getKeyCount() == 1 && parent != null && parent.getKeyCount() == 1 && isSafeToRemoveParent)
            {
                this.nodesCount = this.nodesCount - 2;
                index = parentCursor.index;
                final BTreeNode child = nodesManager.loadNode(1 - index, parent, mappedNode);
                newRoot = nodesManager.copyNode(child, newVersion);

                parentCursor = parentCursor.parent;
            }
            else if (newRoot.getKeyCount() == 1 && parent != null && parent.getKeyCount() > 1)
            {
                this.nodesCount--;
                final BTreeNodeHeap parentCopy = nodesManager.copyNode(parent, newVersion);
                index = parentCursor.index;
                parentCopy.remove(index);

                final int logIndex = ((BTreeNodeAbstract) parentCopy).logBinarySearch(key);
                if (logIndex > 0)
                {
                    ((BTreeNodeAbstract) parentCopy).removeLogKeyValue(logIndex);
                }

                newRoot = parentCopy;

                parentCursor = parentCursor.parent;
            }
            else
            {
                final BTreeNodeHeap copyNode = nodesManager.copyNode(newRoot, newVersion);
                copyNode.remove(index);
                newRoot = copyNode;
            }
        }

        //remove from parent logs
        while (parentCursor != null)
        {
            index = parentCursor.index;

            final BTreeNode parentNode = parentCursor.getNode(mappedNode);
            if (parentNode.getNodeType() == BtreeNodeType.NonLeaf)
            {
                final int logIndex = ((BTreeNodeAbstract) parentNode).logBinarySearch(key);
                if (wasFound)
                {
                    final BTreeNodeHeap copyParent = nodesManager.copyNode(parentNode, newVersion);
                    copyParent.setChild(index, (BTreeNodeHeap) newRoot);

                    if (logIndex >= 0)
                    {
                        ((BTreeNodeAbstract) copyParent).removeLogKeyValue(logIndex);
                    }

                    newRoot = copyParent;
                }
                else
                {
                    if (logIndex >= 0) //found in current node
                    {
                        wasFound = true;
                        final BTreeNodeHeap copyParent = nodesManager.copyNode(parentNode, newVersion);
                        ((BTreeNodeAbstract) copyParent).removeLogKeyValue(logIndex);

                        newRoot = copyParent;
                    }
                }

            }
            else
            {
                throw new IllegalStateException("Should not have leafs in the middle of the tree.");
            }

            parentCursor = parentCursor.parent;
        }

        nodesManager.returnMappedNode(mappedNode);

        if (wasFound)
        {
            writeVersion = newVersion;
            setNewRoot((BTreeNodeHeap) newRoot);
        }
    }

    public void putWithLog(final long key, final long value)
    {
        BTreeNode currentNode;
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final long newVersion = writeVersion++;

        final RootReference rootReference = uncommittedRoot.get();
        if (rootReference != null && rootReference.root != null)
        {
            currentNode = rootReference.root;
        }
        else
        {
            mappedNode.initNode(committedRoot.get());
            currentNode = mappedNode;
        }
        BTreeNodeHeap newRoot = nodesManager.copyNode(currentNode, newVersion);
        nodesManager.returnMappedNode(mappedNode);

        putWithLogInternal(null, -1, newRoot, key, value);

        int keyCount = newRoot.getKeyCount();
        if (keyCount > Config.MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = newRoot.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(newRoot, at, newVersion);
            final BTreeNodeHeap temp = nodesManager.createEmptyNonLeafNode();

            temp.insertChild(0, keyAt, newRoot);
            temp.setChild(1, split);

            newRoot = temp;
        }

        setNewRoot(newRoot);
    }

    private void putWithLogInternal(
            final BTreeNodeHeap parent,
            final int nodeIndexInParent,
            final BTreeNodeHeap node,
            final long key,
            final long value)
    {
        if (node.getNodeType() == BtreeNodeType.Leaf)
        {
            node.insert(key, value);
        }
        else
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            if (nonLeaf.logHasFreeSpace())
            {
                nonLeaf.insertLog(key, value);
            }
            else
            {
                final int keyIndex = nonLeaf.getKeyIndex(key);
                final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(nonLeaf, keyIndex);
                nonLeaf.setChild(keyIndex, childrenCopy);
                putWithLogInternal(nonLeaf, keyIndex, childrenCopy, key, value);

                final long[] keyValues = nonLeaf.spillLog();
                assert keyValues.length % 2 == 0 : "log key/value array must even size. Current size " + keyValues.length;
                final int maxIndex = keyValues.length / 2;

                for (int i = 0; i < maxIndex; i++)
                {
                    final int index = i * 2;
                    final long key2 = keyValues[index];
                    final long value2 = keyValues[index + 1];
                    final int keyIndex2 = nonLeaf.getKeyIndex(key2);
                    final BTreeNodeHeap childrenCopy2 = getOrCreateChildrenCopy(nonLeaf, keyIndex2);
                    nonLeaf.setChild(keyIndex2, childrenCopy2);
                    putWithLogInternal(nonLeaf, keyIndex2, childrenCopy2, key2, value2);
                }
            }
        }

        if (parent != null)
        {
            splitIfRequired(parent, nodeIndexInParent, node, parent.getVersion());
        }
    }

    private void splitIfRequired(
            final BTreeNodeHeap parent,
            final int nodeIndexInParent,
            final BTreeNodeHeap node,
            final long newVersion)
    {
        assert parent != null : "Parent cannot be null when trying to split node";
        assert nodeIndexInParent >= 0 : "the index in parent node must be bigger than 0";
        assert node != null : "Cannot split null node";

        int keyCount = node.getKeyCount();
        if (keyCount > Config.MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = node.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(node, at, newVersion);
            parent.setChild(nodeIndexInParent, split);
            parent.insertChild(nodeIndexInParent, keyAt, node);
        }
    }

    private BTreeNodeHeap getOrCreateChildrenCopy(final BTreeNodeNonLeaf parent, final int keyIndex)
    {
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        final BTreeNode child = nodesManager.loadNode(keyIndex, parent, mappedNode);

        final BTreeNodeHeap childrenCopy;
        if (child instanceof BTreeMappedNode || child.getVersion() != parent.getVersion())
        {
            childrenCopy = nodesManager.copyNode(child, parent.getVersion());
        }
        else
        {
            assert child instanceof  BTreeNodeHeap;
            childrenCopy = (BTreeNodeHeap) child;
        }

        nodesManager.returnMappedNode(mappedNode);
        return childrenCopy;
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
        final long newVersion = writeVersion++;

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

        BTreeNode targetNode = cursorPosition.getNode(mappedNode);
        CursorPosition parentCursor = cursorPosition.parent;

        BTreeNodeHeap currentNode = nodesManager.copyNode(targetNode, newVersion);
        currentNode.insert(key, value);

        int keyCount = currentNode.getKeyCount();
        while (keyCount > Config.MAX_CHILDREN_PER_NODE)
        {
            this.nodesCount++;
            final int at = keyCount >> 1;
            final long keyAt = currentNode.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(currentNode, at, newVersion);

            if (parentCursor == null)
            {
                this.nodesCount++;
                final BTreeNodeHeap temp = nodesManager.createEmptyNonLeafNode();

                temp.insertChild(0, keyAt, currentNode);
                temp.setChild(1, split);
                temp.setVersion(newVersion);

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
        final long version = current.getVersion();

        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        while (parentCursor != null)
        {
            BTreeNodeHeap c = currentNode;
            currentNode = nodesManager.copyNode(parentCursor.getNode(mappedNode), version);
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
    private RootReference setNewRoot(final BTreeNodeHeap newRootPage)
    {
        Objects.requireNonNull(newRootPage, "current uncommittedRoot cannot be null");
        final RootReference currentRoot = uncommittedRoot.get();

        //TODO: extract timestamp retriever
        final long timestamp = System.currentTimeMillis();
        final RootReference updatedRootReference = new RootReference(newRootPage, timestamp, newRootPage.getVersion(), currentRoot);
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
