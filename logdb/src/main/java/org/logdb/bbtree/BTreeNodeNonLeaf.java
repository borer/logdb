package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.NodesManager;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract implements BTreeNodeHeap
{
    public static final int NON_COMMITTED_CHILD = Integer.MIN_VALUE;

    private BTreeNodeHeap[] children;

    /**
     * Load constructor.
     */
    public BTreeNodeNonLeaf(final long pageNumber, final HeapMemory memory)
    {
        super(pageNumber, memory);
        this.children =  new BTreeNodeHeap[0];
    }

    /**
     * Copy/Split constructor.
     */
    public BTreeNodeNonLeaf(
            final long pageNumber,
            final HeapMemory memory,
            final int numberOfLogKeyValues,
            final int numberOfKeys,
            final int numberOfValues,
            final BTreeNodeHeap[] children)
    {
        super(pageNumber, memory, numberOfLogKeyValues, numberOfKeys, numberOfValues);
        this.children = children;
    }

    @Override
    public void insert(final long key, final long value)
    {
        throw new RuntimeException(
                String.format("Cannot insert elements in non leaf node. Key to insert %d, value %d", key, value));
    }

    public void insertLog(final long key, final long value)
    {
        final int index = logBinarySearch(key);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            insertLogKeyValue(absIndex, key, value);
        }
        else
        {
            setLogKeyValue(index, key, value);
        }

        setDirty();
    }

    /**
     * try to remove a key/value pair for this node log.
     * @param key the key that identifies the key/value pair to remove from the node log
     * @return true if removed successfully, false if key/value are not in the log.
     */
    public boolean removeLog(final long key)
    {
        final int index = logBinarySearch(key);
        if (index >= 0)
        {
            removeLogKeyValue(index);
            setDirty();

            return true;
        }

        return false;
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        //TODO (handle this better) : this will be replaced once we commit the child page
        setValue(index, NON_COMMITTED_CHILD);

        children[index] = child;

        setDirty();
    }

    @Override
    public BTreeNode getChildAt(final int index)
    {
        return children[index];
    }

    @Override
    public void insertChild(final int index, final long key, final BTreeNodeHeap child)
    {
        final int rawChildPageCount = getNumberOfChildren();
        //this will be replaced once we commit the child page
        insertKeyAndValue(index, key, NON_COMMITTED_CHILD);

        BTreeNodeHeap[] newChildren = new BTreeNodeHeap[rawChildPageCount + 1];
        copyWithGap(children, newChildren, rawChildPageCount, index);
        children = newChildren;
        children[index] = child;

        setDirty();
    }

    @Override
    public int getNumberOfChildren()
    {
        return getKeyCount() + 1;
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKeyAndValue(index, keyCount);

        removeChildReference(index);

        setDirty();
    }

    private void removeChildReference(final int index)
    {
        final int oldChildrenSize = children.length;
        final BTreeNodeHeap[] newChildren = new BTreeNodeHeap[oldChildrenSize - 1];
        assert newChildren.length >= 0
                : String.format("children size after removing index %d was %d", index, newChildren.length);
        copyExcept(children, newChildren, oldChildrenSize, index);
        children = newChildren;
    }

    @Override
    public long get(final long key)
    {
        int index = getKeyIndex(key);
        return getValue(index);
    }

    @Override
    public int getKeyIndex(long key)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return index;
    }

    @Override
    public void copy(final BTreeNodeHeap copyNode)
    {
        assert copyNode instanceof BTreeNodeNonLeaf : "when copying a non leaf node, needs same type";

        MemoryCopy.copy(buffer, copyNode.getBuffer());
        copyNode.initNodeFromBuffer();

        final BTreeNodeHeap[] copyChildren = new BTreeNodeHeap[children.length];
        System.arraycopy(children, 0, copyChildren, 0, children.length);

        final BTreeNodeNonLeaf bTreeNodeNonLeaf = (BTreeNodeNonLeaf) copyNode;
        bTreeNodeNonLeaf.setChildren(copyChildren);
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getKeyCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeNonLeaf : "when splitting a non leaf node, needs same type";

        final int aNumberOfValues = at + 1;
        final int bNumberOfKeys = numberOfKeys - aNumberOfValues;
        final int bNumberOfValues = numberOfValues - aNumberOfValues;

        final BTreeNodeHeap[] aChildren = new BTreeNodeHeap[aNumberOfValues];
        final BTreeNodeHeap[] bChildren = new BTreeNodeHeap[bNumberOfValues];
        System.arraycopy(children, 0, aChildren, 0, aNumberOfValues);
        System.arraycopy(children, aNumberOfValues, bChildren, 0, bNumberOfValues);
        children = aChildren;

        final BTreeNodeNonLeaf bTreeNodeLeaf = (BTreeNodeNonLeaf) splitNode;
        bTreeNodeLeaf.setChildren(bChildren);
        bTreeNodeLeaf.updateNumberOfKeys(bNumberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bNumberOfValues);

        splitKeysAndValues(at, bNumberOfKeys, bTreeNodeLeaf);

        if (numberOfLogKeyValues > 0)
        {
            final long keyAt = getKey(at);
            splitLog(keyAt, bTreeNodeLeaf);
        }

        bTreeNodeLeaf.setDirty();
        setDirty();
    }

    void setChildren(final BTreeNodeHeap[] children)
    {
        this.children = children;
    }

    @Override
    public long commit(final NodesManager nodesManager,
                       final boolean isRoot,
                       final long previousRootPageNumber,
                       final @Milliseconds long timestamp,
                       final @Version long version)
    {
        if (isDirty)
        {
            for (int index = 0; index < children.length; index++)
            {
                final BTreeNode child = children[index];
                if (child != null)
                {
                    final long pageNumber;
                    if (child.isDirty())
                    {
                        pageNumber = child.commit(nodesManager, false, previousRootPageNumber, timestamp, version);
                    }
                    else
                    {
                        pageNumber = child.getPageNumber();
                    }

                    setValue(index, pageNumber);
                    children[index] = null;
                }
            }

            preCommit(isRoot, previousRootPageNumber, timestamp, version);
            this.pageNumber = nodesManager.commitNode(this);
        }

        return this.pageNumber;
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.NonLeaf;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param gapIndex the index of the gap
     */
    private static void copyWithGap(
            final Object[] src,
            final Object[] dst,
            final int oldSize,
            final int gapIndex)
    {
        if (gapIndex > 0)
        {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize)
        {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize - gapIndex);
        }
    }

    /**
     * Copy the elements of an array, and remove one element.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param removeIndex the index of the entry to remove
     */
    private static void copyExcept(
            final Object[] src,
            final Object[] dst,
            final int oldSize,
            final int removeIndex)
    {
        if (removeIndex > 0 && oldSize > 0)
        {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize)
        {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize - removeIndex - 1);
        }
    }

    @Override
    public HeapMemory getBuffer()
    {
        return (HeapMemory)buffer;
    }
}
