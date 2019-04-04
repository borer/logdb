package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryCopy;
import org.borer.logdb.storage.NodesManager;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    public static final int NON_COMMITTED_CHILD = -1;

    private BTreeNode[] children;

    /**
     * Load constructor.
     */
    public BTreeNodeNonLeaf(final long pageNumber, final Memory memory)
    {
        super(pageNumber, memory);
        this.children =  new BTreeNode[0];
    }

    /**
     * Copy/Split constructor.
     */
    public BTreeNodeNonLeaf(
            final long pageNumber,
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues,
            final BTreeNode[] children)
    {
        super(pageNumber, memory, numberOfKeys, numberOfValues);
        this.children = children;
    }

    @Override
    public void insert(final long key, final long value)
    {
        throw new RuntimeException(
                String.format("Cannot insert elements in non leaf node. Key to insert %d, value %d", key, value));
    }

    public void setChild(final int index, final BTreeNode child)
    {
        //TODO (handle this better) : this will be replaced once we commit the child page
        setValue(index, NON_COMMITTED_CHILD);

        children[index] = child;
        setDirty();
    }

    public BTreeNode getChildAt(final int index)
    {
        return children[index];
    }

    void insertChild(final int index, final long key, final BTreeNode child)
    {
        final int rawChildPageCount = getRawChildPageCount();
        insertKey(index, key);
        //this will be replaced once we commit the child page
        insertValue(index, NON_COMMITTED_CHILD);

        BTreeNode[] newChildren = new BTreeNode[rawChildPageCount + 1];
        copyWithGap(children, newChildren, rawChildPageCount, index);
        children = newChildren;
        children[index] = child;
        setDirty();
    }

    int getRawChildPageCount()
    {
        return getKeyCount() + 1;
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKey(index, keyCount);
        removeValue(index, keyCount);

        removeChildReference(index);
        setDirty();
    }

    private void removeChildReference(final int index)
    {
        final int oldChildrenSize = children.length;
        final BTreeNode[] newChildren = new BTreeNode[oldChildrenSize - 1];
        assert newChildren.length >= 0
                : String.format("children size after removing index %d was %d", index, newChildren.length);
        copyExcept(children, newChildren, oldChildrenSize, index);
        children = newChildren;
    }

    @Override
    public long get(final long key)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return getValue(index);
    }

    @Override
    public void copy(final BTreeNode copyNode)
    {
        assert copyNode instanceof BTreeNodeNonLeaf : "when splitting a non leaf node, needs same type";

        final BTreeNodeNonLeaf bTreeNodeNonLeaf = (BTreeNodeNonLeaf) copyNode;
        MemoryCopy.copy(buffer, bTreeNodeNonLeaf.getBuffer());

        bTreeNodeNonLeaf.updateBuffer(bTreeNodeNonLeaf.getBuffer());

        final BTreeNode[] copyChildren = new BTreeNode[children.length];
        System.arraycopy(children, 0, copyChildren, 0, children.length);

        bTreeNodeNonLeaf.setChildren(copyChildren);
    }

    @Override
    public boolean needRebalancing(int threshold)
    {
        return numberOfValues < 2;
    }

    @Override
    public void split(final int at, final BTreeNode splitNode)
    {
        assert getKeyCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeNonLeaf : "when splitting a non leaf node, needs same type";

        final int aNumberOfValues = at + 1;
        final int bNumberOfKeys = numberOfKeys - aNumberOfValues;
        final int bNumberOfValues = numberOfValues - aNumberOfValues;

        final BTreeNode[] aChildren = new BTreeNode[aNumberOfValues];
        final BTreeNode[] bChildren = new BTreeNode[bNumberOfValues];
        System.arraycopy(children, 0, aChildren, 0, aNumberOfValues);
        System.arraycopy(children, aNumberOfValues, bChildren, 0, bNumberOfValues);
        children = aChildren;

        final BTreeNodeNonLeaf bTreeNodeLeaf = (BTreeNodeNonLeaf) splitNode;
        bTreeNodeLeaf.setChildren(bChildren);
        bTreeNodeLeaf.updateNumberOfKeys(bNumberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bNumberOfValues);

        splitKeys(at, bNumberOfKeys, bTreeNodeLeaf);
        splitValues(aNumberOfValues, bNumberOfValues, bTreeNodeLeaf);

        setDirty();
    }

    private void setChildren(final BTreeNode[] children)
    {
        this.children = children;
    }

    @Override
    public long commit(final NodesManager nodesManager)
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
                        pageNumber = child.commit(nodesManager);
                    }
                    else
                    {
                        pageNumber = child.getPageNumber();
                    }

                    setValue(index, pageNumber);
                    children[index] = null;
                }
            }

            preCommit();
            pageNumber = nodesManager.commitNode(this);
            isDirty = false;
        }

        return pageNumber;
    }

    @Override
    protected BtreeNodeType getNodeType()
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
}
