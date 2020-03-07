package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

import java.util.Arrays;

public class BTreeWithLog extends BTreeAbstract
{
    private static final byte[] LOG_VALUE_TO_REMOVE_SENTINEL = BinaryHelper.longToBytes(-1);
    private static final @ByteSize int REQUIRED_SPACE = StorageUnits.size(2 * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
    private static final @ByteSize int REQUIRED_SPACE_LOG = StorageUnits.size(BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);

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
    public void remove(final byte[] key)
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

            if (newRoot.getNodeType() == BtreeNodeType.NonLeaf && newRoot.getPairCount() == 1)
            {
                final int onlyChildIndex = 0;
                final BTreeNode nodeToRemoveFrom = nodesManager.loadNode(onlyChildIndex, newRoot, mappedNode);
                if (nodeToRemoveFrom.getNodeType() == BtreeNodeType.Leaf)
                {
                    this.nodesCount--;
                    newRoot = getOrCreateChildrenCopy((BTreeNodeNonLeaf) newRoot, onlyChildIndex);
                }
                else
                {
                    final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) newRoot;
                    spillLogForRemove(nonLeaf);

                    if (nonLeaf.getPairCount() == 1)
                    {
                        this.nodesCount--;
                        newRoot = getOrCreateChildrenCopy(nonLeaf, onlyChildIndex);
                    }
                }
            }

            setNewRoot(newRoot);
        }
    }

    private void removeWithLogRecursive(
            final BTreeNodeNonLeaf parent,
            final int nodeIndexInParent,
            final BTreeNodeHeap node,
            final byte[] key)
    {
        if (node.getNodeType() == BtreeNodeType.Leaf)
        {
            final int keyIndex = node.getKeyIndex(key);
            if (keyIndex >= 0)
            {
                node.removeAtIndex(keyIndex);
            }
        }
        else
        {
            final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            if (nonLeaf.logHasFreeSpace(REQUIRED_SPACE_LOG))
            {
                nonLeaf.insertLog(key, LOG_VALUE_TO_REMOVE_SENTINEL);
            }
            else
            {
                removeWithLogRecursiveAndRebalanceParent(nonLeaf, key);

                //remove log if there is any, as we already removed the key/value in the previous step
                nonLeaf.removeLog(key);

                spillLogForRemove(nonLeaf);
            }
        }

        if (node.getPairCount() == 0 && parent != null)
        {
            this.nodesCount--;
            parent.removeAtIndex(nodeIndexInParent);
        }
    }

    private void spillLogForRemove(final BTreeNodeNonLeaf node)
    {
        final KeyValueHeapImpl keyValueLog = node.spillLog();
        for (int i = 0; i < keyValueLog.getNumberOfPairs(); ++i)
        {
            final byte[] logKey = keyValueLog.getKeyAtIndex(i);
            final byte[] logValue = keyValueLog.getValueAtIndex(i);

            final boolean shouldRemoveValue = Arrays.equals(logValue, LOG_VALUE_TO_REMOVE_SENTINEL);
            if (shouldRemoveValue)
            {
                removeWithLogRecursiveAndRebalanceParent(node, logKey);
            }
            else
            {
                putWithLogRecursiveInChildAndSplitIfRequired(node, logKey, logValue);
            }
        }
    }

    private void removeWithLogRecursiveAndRebalanceParent(final BTreeNodeNonLeaf parent, final byte[] logKey)
    {
        final int keyIndex = parent.getKeyIndex(logKey);
        final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(parent, keyIndex);
        parent.setChild(keyIndex, childrenCopy);

        removeWithLogRecursive(parent, keyIndex, childrenCopy, logKey);
    }

    /**
     * Removes the key by first walking the tree and searching if it actually has a value with that key.
     * If no value is found this operation is a no-op.
     * @param key the key that identifies the pair to remove.
     */
    public void removeWithoutFalsePositives(final byte[] key)
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
                BTreeNode parent = null;
                if (parentCursor != null)
                {
                    parent = parentCursor.getNode(mappedNode);
                    assert parent.getNodeType() == BtreeNodeType.NonLeaf;
                }

                final BTreeNodeHeap copyNode = nodesManager.copyNode(newRoot, newVersion);
                copyNode.removeAtIndex(index);

                if (copyNode.getPairCount() == 0 && parent != null)
                {
                    this.nodesCount--;
                    final BTreeNodeHeap parentCopy = nodesManager.copyNode(parent, newVersion);
                    index = parentCursor.index;
                    parentCopy.removeAtIndex(index);
                    ((BTreeLogNode) parentCopy).removeLog(key);

                    newRoot = parentCopy;

                    parentCursor = parentCursor.parent;
                }
                else
                {
                    newRoot = copyNode;
                }
            }

            //NOTE: remove from internal nodes (parents of the leaf node)
            while (parentCursor != null)
            {
                index = parentCursor.index;

                BTreeNode parentNode = parentCursor.getNode(mappedNode);
                if (parentNode.getNodeType() == BtreeNodeType.NonLeaf)
                {
                    BTreeNodeHeap copyParent = null;
                    if (parentNode.getPairCount() == 1)
                    {
                        spillLogForRemove((BTreeNodeNonLeaf) parentNode);

                        this.nodesCount--;

                        if (parentCursor.parent != null)
                        {
                            parentCursor = parentCursor.parent;
                            index = parentCursor.index;

                            final BTreeNode parentCursorNode = parentCursor.getNode(mappedNode);
                            copyParent = nodesManager.copyNode(parentCursorNode, newVersion);
                            copyParent.setChild(index, (BTreeNodeHeap) newRoot);
                        }
                        else
                        {
                            break;
                        }
                    }

                    final boolean hasLogValue = ((BTreeLogNode) parentNode).hasKeyLog(key);
                    if (wasFound)
                    {
                        if (copyParent == null)
                        {
                            copyParent = nodesManager.copyNode(parentNode, newVersion);
                        }

                        copyParent.setChild(index, (BTreeNodeHeap) newRoot);

                        if (hasLogValue)
                        {
                            ((BTreeLogNode) copyParent).removeLog(key);
                        }

                        newRoot = copyParent;
                    }
                    else
                    {
                        if (hasLogValue) //found in current node
                        {
                            wasFound = true;
                            if (copyParent == null)
                            {
                                copyParent = nodesManager.copyNode(parentNode, newVersion);
                            }

                            ((BTreeLogNode) copyParent).removeLog(key);

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
                if (newRoot.getNodeType() == BtreeNodeType.NonLeaf && newRoot.getPairCount() == 1)
                {
                    final BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) newRoot;
                    spillLogForRemove(nonLeaf);
                    this.nodesCount--;
                    newRoot = getOrCreateChildrenCopy(nonLeaf, 0);
                }

                nextWriteVersion = newVersion;
                setNewRoot((BTreeNodeHeap) newRoot);
            }
        }
    }

    @Override
    public void put(final byte[] key, final byte[] value)
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

        if (newRoot.shouldSplit(REQUIRED_SPACE))
        {
            this.nodesCount = this.nodesCount + 2;
            int keyCount = newRoot.getPairCount();
            final int at = keyCount >> 1;
            final byte[] keyAt = newRoot.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(newRoot, at, newVersion);
            final BTreeNodeNonLeaf parent = nodesManager.createEmptyNonLeafNode();
            parent.setVersion(newVersion);

            parent.insertChild(0, keyAt, newRoot);
            parent.setChild(1, split);

            final int compare = ByteArrayComparator.INSTANCE.compare(split.getMinKey(), key);

            final BTreeNodeHeap rootToPut = compare > 0 ? newRoot : split;
            putWithLogRecursive(parent, at, rootToPut, key, value);

            setNewRoot(parent);
        }
        else
        {
            putWithLogRecursive(null, -1, newRoot, key, value);
            setNewRoot(newRoot);
        }
    }

    private void putWithLogRecursive(
            final BTreeNodeNonLeaf parent,
            final int nodeIndexInParent,
            final BTreeNodeHeap node,
            final byte[] key,
            final byte[] value)
    {
        if (node.getNodeType() == BtreeNodeType.Leaf)
        {
            node.insert(key, value);
        }
        else
        {
            assert node instanceof BTreeNodeNonLeaf : "Node is not instance of BTreeNodeNonLeaf. " + node.toString();

            BTreeNodeNonLeaf currentNonLeaf = (BTreeNodeNonLeaf) node;
            if (currentNonLeaf.logHasFreeSpace(REQUIRED_SPACE_LOG))
            {
                currentNonLeaf.insertLog(key, value);
            }
            else
            {
                putWithLogRecursiveInChildAndSplitIfRequired(currentNonLeaf, key, value);

                //remove log if there is any, as we already removed the key/value in the previous step
                currentNonLeaf.removeLog(key);

                spillLogForPut(currentNonLeaf);
            }

            if (currentNonLeaf.shouldSplit(REQUIRED_SPACE) && parent != null)
            {
                this.nodesCount++;
                final int nodeKeyCount = currentNonLeaf.getPairCount();
                final int nodeKeyIndex = nodeKeyCount >> 1;
                final byte[] splitKey = currentNonLeaf.getKey(nodeKeyIndex);

                //split current nonleaf into children
                final BTreeNodeHeap nonLeafSplit = nodesManager.splitNode(currentNonLeaf, nodeKeyIndex, currentNonLeaf.getVersion());

                parent.insertChild(nodeIndexInParent, splitKey, currentNonLeaf);
                parent.setChild(nodeIndexInParent +  1, nonLeafSplit);
            }
        }
    }

    private void putWithLogRecursiveInChildAndSplitIfRequired(final BTreeNodeNonLeaf parent, final byte[] key, final byte[] value)
    {
        int keyIndex = parent.getKeyIndex(key);
        final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(parent, keyIndex);

        if (childrenCopy.shouldSplit(REQUIRED_SPACE))
        {
            this.nodesCount++;
            final int childrenKeyCount = childrenCopy.getPairCount();
            final int at = childrenKeyCount >> 1;
            final byte[] splitKey = childrenCopy.getKey(at);

            final BTreeNodeHeap childrenSplit = nodesManager.splitNode(childrenCopy, at, parent.getVersion());
            parent.insertChild(keyIndex, splitKey, childrenCopy);
            parent.setChild(keyIndex + 1, childrenSplit);

            final int compare = ByteArrayComparator.INSTANCE.compare(childrenSplit.getMinKey(), key);
            if (compare > 0)
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

    private void spillLogForPut(final BTreeNodeNonLeaf nonLeaf)
    {
        final KeyValueHeapImpl keyValueLog = nonLeaf.spillLog();
        for (int i = 0; i < keyValueLog.getNumberOfPairs(); ++i)
        {
            final byte[] logKey = keyValueLog.getKeyAtIndex(i);
            final byte[] logValue = keyValueLog.getValueAtIndex(i);

            final boolean shouldRemoveValue = Arrays.equals(logValue, LOG_VALUE_TO_REMOVE_SENTINEL);
            if (shouldRemoveValue)
            {
                removeWithLogRecursiveAndRebalanceParent(nonLeaf, logKey);
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
    public byte[] get(final byte[] key)
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
    public byte[] get(final byte[] key, final @Version long version)
    {
        assert version >= 0;

        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode rootNode = getRootNode(version, mappedNode);
            return getKey(key, rootNode);
        }
    }

    @Override
    public byte[] getByTimestamp(final byte[] key, @Milliseconds long timestamp)
    {
        assert timestamp >= 0;

        try (BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode())
        {
            final BTreeNode rootNode = getRootNodeByTimestamp(timestamp, mappedNode);
            return getKey(key, rootNode);
        }
    }

    private byte[] getKey(final byte[] key, final BTreeNode root)
    {
        try (BTreeMappedNode  mappedNode = nodesManager.getOrCreateMappedNode())
        {
            BTreeNode currentNode = root;
            while (currentNode.getNodeType() == BtreeNodeType.NonLeaf)
            {
                final BTreeLogNode bTreeLogNode = (BTreeLogNode) currentNode;
                if (bTreeLogNode.getLogKeyValuesCount() > 0)
                {
                    if (bTreeLogNode.hasKeyLog(key))
                    {
                        final byte[] logValue = bTreeLogNode.getLogValue(key);
                        if (Arrays.equals(logValue, LOG_VALUE_TO_REMOVE_SENTINEL))
                        {
                            return null;
                        }
                        else
                        {
                            return logValue;
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
