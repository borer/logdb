package org.logdb.bbtree;

import org.logdb.storage.NodesManager;

public class BTreeWithLog extends BTreeAbstract
{
    private static final int LOG_VALUE_TO_REMOVE_SENTINEL = -1;

    public BTreeWithLog(NodesManager nodesManager)
    {
        super(nodesManager);
    }

    /**
     * Removes the key from the tree by appending a thombstone without touching leafs.
     * When the tree is spilled, the entry will be actually removed from the leaf.
     * @param key the key that identifies the pair to remove.
     */
    @Override
    public void remove(final long key)
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
            final int keyIndex = newRoot.getKeyIndex(key);
            final BTreeNode nodeToRemoveFrom = nodesManager.loadNode(keyIndex, newRoot, mappedNode);
            if (nodeToRemoveFrom.getNodeType() == BtreeNodeType.Leaf && nodeToRemoveFrom.getKeyCount() == 0)
            {
                this.nodesCount = this.nodesCount - 2;
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
                nonLeaf.insertLog(key, LOG_VALUE_TO_REMOVE_SENTINEL);
            }
            else
            {
                final int keyIndex = nonLeaf.getKeyIndex(key);
                final BTreeNodeHeap childrenCopy = getOrCreateChildrenCopy(nonLeaf, keyIndex);
                nonLeaf.setChild(keyIndex, childrenCopy);
                removeWithLogInternal(nonLeaf, keyIndex, childrenCopy, key);

                final int childKeyIndex = nonLeaf.getKeyIndex(key);
                final BTreeNodeHeap childToRemove = getOrCreateChildrenCopy(nonLeaf, childKeyIndex);
                if (childToRemove.getKeyCount() == 0 && nonLeaf.getKeyCount() > 1)
                {
                    this.nodesCount--;
                    nonLeaf.remove(keyIndex);
                }
                else if (childToRemove.getKeyCount() == 0 && nonLeaf.getKeyCount() == 1 && parent != null)
                {
                    this.nodesCount = this.nodesCount - 2;
                    final BTreeNodeHeap childrenCopy3 = getOrCreateChildrenCopy(nonLeaf, 1 - childKeyIndex);
                    parent.setChild(nodeIndexInParent, childrenCopy3);
                }

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

                    final boolean shouldRemoveValue = value2 == LOG_VALUE_TO_REMOVE_SENTINEL;
                    if (shouldRemoveValue)
                    {
                        removeWithLogInternal(nonLeaf, keyIndex2, childrenCopy2, key2);

                        final int childKeyIndex2 = nonLeaf.getKeyIndex(key2);
                        final BTreeNodeHeap childToRemove2 = getOrCreateChildrenCopy(nonLeaf, childKeyIndex2);
                        if (childToRemove2.getKeyCount() == 0 && nonLeaf.getKeyCount() > 1)
                        {
                            this.nodesCount--;
                            nonLeaf.remove(childKeyIndex2);
                        }
                        else if (childToRemove2.getKeyCount() == 0 && nonLeaf.getKeyCount() == 1 && parent != null)
                        {
                            this.nodesCount = this.nodesCount - 2;
                            final BTreeNodeHeap childrenCopy3 = getOrCreateChildrenCopy(nonLeaf, 1 - childKeyIndex2);
                            parent.setChild(nodeIndexInParent, childrenCopy3);
                        }
                    }
                    else
                    {
                        putWithLogInternal(nonLeaf, keyIndex2, childrenCopy2, key2, value2);
                    }
                }
            }
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

    @Override
    public void put(final long key, final long value)
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

        if (newRoot.shouldSplit())
        {
            this.nodesCount = this.nodesCount + 2;
            int keyCount = newRoot.getKeyCount();
            final int at = keyCount >> 1;
            final long keyAt = newRoot.getKey(at);
            final BTreeNodeHeap split = nodesManager.splitNode(newRoot, at, newVersion);
            final BTreeNodeHeap parent = nodesManager.createEmptyNonLeafNode();

            parent.insertChild(0, keyAt, newRoot);
            parent.setChild(1, split);

            newRoot = split.getMinKey() > key ? newRoot : split;
            putWithLogInternal(parent, at, newRoot, key, value);

            setNewRoot(parent);
        }
        else
        {
            putWithLogInternal(null, -1, newRoot, key, value);
            setNewRoot(newRoot);
        }
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
            BTreeNodeNonLeaf nonLeaf = (BTreeNodeNonLeaf) node;
            if (nonLeaf.logHasFreeSpace())
            {
                nonLeaf.insertLog(key, value);
            }
            else
            {
                final long[] keyValues = nonLeaf.spillLog();
                assert keyValues.length % 2 == 0 : "log key/value array must even size. Current size " + keyValues.length;
                final int maxIndex = keyValues.length / 2;

                splitIfRequiredAndPutWithLogInternal(nonLeaf, key, value);

                for (int i = 0; i < maxIndex; i++)
                {
                    final int index = i * 2;
                    final long key2 = keyValues[index];
                    final long value2 = keyValues[index + 1];

                    splitIfRequiredAndPutWithLogInternal(nonLeaf, key2, value2);
                }
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

    private void splitIfRequiredAndPutWithLogInternal(
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
                putWithLogInternal(parent, keyIndex - 1, childrenCopy, key, value);
            }
            else
            {
                putWithLogInternal(parent, keyIndex, childrenSplit, key, value);
            }
        }
        else
        {
            parent.setChild(keyIndex, childrenCopy);
            putWithLogInternal(parent, keyIndex, childrenCopy, key, value);
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

    @Override
    public long get(final long key)
    {
        BTreeNode currentNode;
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();

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

        final long value = getKey(key, currentNode);
        nodesManager.returnMappedNode(mappedNode);

        return value;
    }

    /**
     * Gets a value for the key at time/instance t.
     *
     * @param key     the key to search for
     * @param version the version that we are interested. Must be >= 0
     */
    @Override
    public long get(final long key, final int version)
    {
        BTreeNode currentNode;
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        currentNode = getRootNode(version, mappedNode);

        final long value = getKey(key, currentNode);
        nodesManager.returnMappedNode(mappedNode);

        return value;
    }

    private long getKey(final long key, final BTreeNode root)
    {
        final BTreeMappedNode mappedNode = nodesManager.getOrCreateMappedNode();
        BTreeNode currentNode = root;

        while (currentNode.getNodeType() == BtreeNodeType.NonLeaf)
        {
            if (((BTreeNodeAbstract) currentNode).getLogKeyValuesCount() > 0)
            {
                final int logIndex = ((BTreeNodeAbstract) currentNode).logBinarySearch(key);
                if (logIndex >= 0)
                {
                    return ((BTreeNodeAbstract) currentNode).getLogValue(logIndex);
                }
            }
            final int keyIndex = currentNode.getKeyIndex(key);
            currentNode = nodesManager.loadNode(keyIndex, currentNode, mappedNode);
        }

        final long value = currentNode.get(key);
        nodesManager.returnMappedNode(mappedNode);
        return value;
    }

    private BTreeNode getRootNode(final int version, final BTreeMappedNode mappedNode)
    {
        final BTreeNode rootForVersion;
        final RootReference currentRootReference = uncommittedRoot.get();
        if (currentRootReference != null)
        {
            final RootReference rootNodeForVersion = currentRootReference.getRootReferenceForVersion(version);
            if (rootNodeForVersion != null)
            {
                rootForVersion = rootNodeForVersion.root;
            }
            else
            {
                mappedNode.initNode(committedRoot.get());
                rootForVersion = mappedNode;
            }
        }
        else
        {
            mappedNode.initNode(committedRoot.get());
            rootForVersion = mappedNode;
        }

        if (rootForVersion.getVersion() != version)
        {
            throw new IllegalArgumentException("Didn't have version " + version);
        }

        return rootForVersion;
    }
}
