package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.TimeSource;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class BTreeWithLog extends BTreeAbstract
{
    private static final int LOG_VALUE_TO_REMOVE_SENTINEL = -1;

    public BTreeWithLog(
            final NodesManager nodesManager,
            final TimeSource timeSource,
            final @Version long nextWriteVersion,
            final @PageNumber long lastRootPageNumber,
            final RootReference rootReference)
    {
        super(nodesManager, timeSource, nextWriteVersion, lastRootPageNumber, rootReference);
    }

    /**
     * Removes the key from the tree by appending a thombstone without touching leafs.
     * When the tree is spilled, the entry will be actually removed from the leaf.
     * @param key the key that identifies the pair to remove.
     */
    @Override
    public void remove(final long key)
    {
        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            BTreeNode currentNode;
            final @Version long newVersion = nextWriteVersion++;

            final RootReference rootReference = uncommittedRoot.get();
            if (rootReference != null && rootReference.root != null)
            {
                currentNode = rootReference.root;
            }
            else
            {
                mappedNode.initNode(StorageUnits.pageNumber(committedRoot.get()));
                currentNode = mappedNode;
            }
            BTreeNodeHeap newRoot = nodesManager.copyNode(currentNode, newVersion);

            removeWithLogRecursive(null, -1, newRoot, key);

            if (newRoot.getNodeType() == BtreeNodeType.NonLeaf && newRoot.getKeyCount() == 1)
            {
                final int keyIndex = newRoot.getKeyIndex(key);
                final BTreeNode nodeToRemoveFrom = nodesManager.loadNode(keyIndex, newRoot, mappedNode);
                if (nodeToRemoveFrom.getNodeType() == BtreeNodeType.Leaf && nodeToRemoveFrom.getKeyCount() == 0)
                {
                    this.nodesCount = this.nodesCount - 2;
                    newRoot = getOrCreateChildrenCopy((BTreeNodeNonLeaf) newRoot, 1 - keyIndex);
                }
            }

            setNewRoot(newRoot);
        }
    }

    private void removeWithLogRecursive(
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
                nonLeaf.insertLog(key, LOG_VALUE_TO_REMOVE_SENTINEL);
            }
            else
            {
                final int keyIndex = nonLeaf.getKeyIndex(key);
                final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(nonLeaf, keyIndex);
                nonLeaf.setChild(keyIndex, childrenCopy);
                removeWithLogRecursiveAndRebalanceParent(parent, nodeIndexInParent, nonLeaf, key, keyIndex, childrenCopy);

                //remove log if there is any, as we already removed the key/value in the previous step
                nonLeaf.removeLog(key);

                spillLogForRemove(parent, nodeIndexInParent, nonLeaf);
            }
        }
    }

    private void spillLogForRemove(
            final BTreeNodeHeap parent,
            final int nodeIndexInParent,
            final BTreeNodeNonLeaf node)
    {
        final HeapMemory keyValues = node.spillLog();
        for (int i = 0; i < KeyValueLog.getNumberOfPairs(keyValues.getCapacity()); ++i)
        {
            final long logKey = KeyValueLog.getKey(keyValues, i);
            final long logValue = KeyValueLog.getValue(keyValues, i);

            final int logKeyIndex = node.getKeyIndex(logKey);
            final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(node, logKeyIndex);
            node.setChild(logKeyIndex, childrenCopy);

            final boolean shouldRemoveValue = logValue == LOG_VALUE_TO_REMOVE_SENTINEL;
            if (shouldRemoveValue)
            {
                removeWithLogRecursiveAndRebalanceParent(parent, nodeIndexInParent, node, logKey, logKeyIndex, childrenCopy);
            }
            else
            {
                putWithLogRecursive(node, logKeyIndex, childrenCopy, logKey, logValue);
            }
        }
    }

    private void removeWithLogRecursiveAndRebalanceParent(
            final BTreeNodeHeap grandparent,
            final int parentIndexInGrandparent,
            final BTreeNodeNonLeaf parent,
            final long logKey,
            final int keyIndexInParent,
            final BTreeNodeHeap node)
    {
        removeWithLogRecursive(parent, keyIndexInParent, node, logKey);

        final int childKeyIndex = parent.getKeyIndex(logKey);
        final BTreeNodeHeap childToRemove = getOrCreateChildrenCopy(parent, childKeyIndex);
        if (childToRemove.getKeyCount() == 0 && parent.getKeyCount() > 1)
        {
            this.nodesCount--;
            parent.remove(childKeyIndex);
        }
        else if (childToRemove.getKeyCount() == 0 && parent.getKeyCount() == 1 && grandparent != null)
        {
            this.nodesCount = this.nodesCount - 2;
            final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(parent, 1 - childKeyIndex);
            grandparent.setChild(parentIndexInGrandparent, childrenCopy);
        }
    }

    /**
     * Removes the key by first walking the tree and searching if it actually has a value with that key.
     * If no value is found this operation is a no-op.
     * @param key the key that identifies the pair to remove.
     */
    public void removeWithoutFalsePositives(final long key)
    {
        final CursorPosition cursorPosition = getLastCursorPosition(key);
        final @Version long newVersion = StorageUnits.version(nextWriteVersion++);

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            int index = cursorPosition.index;
            BTreeNode newRoot = cursorPosition.getNode(mappedNode);
            CursorPosition parentCursor = cursorPosition.parent;
            boolean wasFound = false;

            //NOTE: remove from leaf node
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

                //Note: if current leaf has only one element, try to remove directly from parent
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

                    final int logIndex = ((BTreeNodeAbstract) parentCopy).binarySearchInLog(key);
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

            //NOTE: remove from internal nodes (parents of the leaf node)
            while (parentCursor != null)
            {
                index = parentCursor.index;

                final BTreeNode parentNode = parentCursor.getNode(mappedNode);
                if (parentNode.getNodeType() == BtreeNodeType.NonLeaf)
                {
                    final int logIndex = ((BTreeNodeAbstract) parentNode).binarySearchInLog(key);
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

            if (wasFound)
            {
                nextWriteVersion = newVersion;
                setNewRoot((BTreeNodeHeap) newRoot);
            }
        }
    }

    @Override
    public void put(final long key, final long value)
    {
        BTreeNodeHeap newRoot;
        final @Version long newVersion = nextWriteVersion++;

        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            BTreeNode currentNode;

            final RootReference rootReference = uncommittedRoot.get();
            if (rootReference != null && rootReference.root != null)
            {
                currentNode = rootReference.root;
            }
            else
            {
                mappedNode.initNode(StorageUnits.pageNumber(committedRoot.get()));
                currentNode = mappedNode;
            }
            newRoot = nodesManager.copyNode(currentNode, newVersion);
        }

        if (newRoot.shouldSplit())
        {
            this.nodesCount = this.nodesCount + 2;
            int keyCount = newRoot.getKeyCount();
            final int at = keyCount >> 1;
            final long keyAt = newRoot.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(newRoot, at, newVersion);
            final BTreeNodeHeap parent = nodesManager.createEmptyNonLeafNode();
            parent.setVersion(newVersion);

            parent.insertChild(0, keyAt, newRoot);
            parent.setChild(1, split);

            newRoot = split.getMinKey() > key ? newRoot : split;
            putWithLogRecursive(parent, at, newRoot, key, value);

            setNewRoot(parent);
        }
        else
        {
            putWithLogRecursive(null, -1, newRoot, key, value);
            setNewRoot(newRoot);
        }
    }

    private void putWithLogRecursive(
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
            assert node instanceof BTreeNodeNonLeaf : "Node is not instance of BTreeNodeNonLeaf. " + node.toString();

            BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            if (nonLeaf.logHasFreeSpace())
            {
                nonLeaf.insertLog(key, value);
            }
            else
            {
                putWithLogRecursiveInChildAndSplitIfRequired(nonLeaf, key, value);

                //remove log if there is any, as we already removed the key/value in the previous step
                nonLeaf.removeLog(key);

                spillLogForPut(parent, nodeIndexInParent, nonLeaf);
            }

            if (nonLeaf.shouldSplit() && parent != null)
            {
                this.nodesCount++;
                final int nodeKeyCount = nonLeaf.getKeyCount();
                final int nodeKeyIndex = nodeKeyCount >> 1;
                final long splitKey = nonLeaf.getKey(nodeKeyIndex);

                //split current nonleaf into children
                final BTreeNodeHeap nonLeafSplit = nodesManager.splitNode(nonLeaf, nodeKeyIndex, nonLeaf.getVersion());

                parent.insertChild(nodeIndexInParent, splitKey, nonLeaf);
                parent.setChild(nodeIndexInParent +  1, nonLeafSplit);
            }
        }
    }

    private void putWithLogRecursiveInChildAndSplitIfRequired(
            final BTreeNodeNonLeaf parent,
            final long key,
            final long value)
    {
        int keyIndex = parent.getKeyIndex(key);
        final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(parent, keyIndex);

        if (childrenCopy.shouldSplit())
        {
            this.nodesCount++;
            final int childrenKeyCount = childrenCopy.getKeyCount();
            final int at = childrenKeyCount >> 1;
            final long splitKey = childrenCopy.getKey(at);

            final BTreeNodeHeap childrenSplit = nodesManager.splitNode(childrenCopy, at, parent.getVersion());
            parent.insertChild(keyIndex, splitKey, childrenCopy);
            parent.setChild(keyIndex + 1, childrenSplit);

            if (childrenSplit.getMinKey() > key)
            {
                putWithLogRecursive(parent, keyIndex - 1, childrenCopy, key, value);
            }
            else
            {
                putWithLogRecursive(parent, keyIndex, childrenSplit, key, value);
            }
        }
        else
        {
            parent.setChild(keyIndex, childrenCopy);
            putWithLogRecursive(parent, keyIndex, childrenCopy, key, value);
        }
    }

    private void spillLogForPut(
            final BTreeNodeHeap parent,
            final int nodeIndexInParent,
            final BTreeNodeNonLeaf nonLeaf)
    {
        final HeapMemory keyValues = nonLeaf.spillLog();
        for (int i = 0; i < KeyValueLog.getNumberOfPairs(keyValues.getCapacity()); ++i)
        {
            final long logKey = KeyValueLog.getKey(keyValues, i);
            final long logValue = KeyValueLog.getValue(keyValues, i);

            final boolean shouldRemoveValue = logValue == LOG_VALUE_TO_REMOVE_SENTINEL;
            if (shouldRemoveValue)
            {
                removeWithLogRecursive(parent, nodeIndexInParent, nonLeaf, logKey);
            }
            else
            {
                putWithLogRecursiveInChildAndSplitIfRequired(nonLeaf, logKey, logValue);
            }
        }
    }

    private BTreeNodeHeap getOrCreateChildrenCopy(final BTreeNodeNonLeaf parent, final int keyIndex)
    {
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {

            final BTreeNode child = nodesManager.loadNode(keyIndex, parent, mappedNode);

            final BTreeNodeHeap childrenCopy;
            if (child instanceof BTreeMappedNode || child.getVersion() != parent.getVersion())
            {
                childrenCopy = nodesManager.copyNode(child, parent.getVersion());
            }
            else
            {
                assert child instanceof BTreeNodeHeap;
                childrenCopy = (BTreeNodeHeap) child;
            }

            return childrenCopy;
        }
    }

    @Override
    public long get(final long key)
    {
        BTreeNode currentNode;
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {

            final RootReference rootReference = uncommittedRoot.get();
            if (rootReference != null && rootReference.root != null)
            {
                currentNode = rootReference.root;
            }
            else
            {
                mappedNode.initNode(StorageUnits.pageNumber(committedRoot.get()));
                currentNode = mappedNode;
            }

            return getKey(key, currentNode);
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
        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode currentNode = getRootNode(version, mappedNode);
            return getKey(key, currentNode);
        }
    }

    private long getKey(final long key, final BTreeNode root)
    {
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            BTreeNode currentNode = root;
            while (currentNode.getNodeType() == BtreeNodeType.NonLeaf)
            {
                if (((BTreeNodeAbstract) currentNode).getLogKeyValuesCount() > 0)
                {
                    final int logIndex = ((BTreeNodeAbstract) currentNode).binarySearchInLog(key);
                    if (logIndex >= 0)
                    {
                        final long logValue = ((BTreeNodeAbstract) currentNode).getLogValue(logIndex);
                        if (logValue != LOG_VALUE_TO_REMOVE_SENTINEL)
                        {
                            return logValue;
                        }
                        else
                        {
                            return KEY_NOT_FOUND_VALUE;
                        }
                    }
                }
                final int keyIndex = currentNode.getKeyIndex(key);
                currentNode = nodesManager.loadNode(keyIndex, currentNode, mappedNode);
            }

            return currentNode.get(key);
        }
    }
}
