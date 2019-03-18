package org.borer.logdb;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryCopy;
import org.borer.logdb.bit.MemoryFactory;

import java.util.function.LongSupplier;

import static org.borer.logdb.Config.BYTE_ORDER;
import static org.borer.logdb.Config.PAGE_SIZE_BYTES;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    BTreeNode[] children;

    BTreeNodeNonLeaf(final BTreeNode child)
    {
        super(
                MemoryFactory.allocateDirect(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                1, //there is always one child at least
                new IdSupplier());
        this.children = new BTreeNode[1]; //there is always one child at least

        setChild(0, child);
    }

    /**
     * split constructor.
     */
    BTreeNodeNonLeaf(
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues,
            final BTreeNode[] children,
            final LongSupplier idSupplier)
    {
        super(memory, numberOfKeys, numberOfValues, idSupplier);
        this.children = children;
    }

    /**
     * Copy constructor.
     */
    private BTreeNodeNonLeaf(
            final long id,
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues,
            final BTreeNode[] children,
            final LongSupplier idSupplier)
    {
        super(id, memory, numberOfKeys, numberOfValues, idSupplier);
        this.children = children;
    }

    @Override
    public void insert(final long key, final long value)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        children[index].insert(key, value);
    }

    void setChild(final int index, final BTreeNode child)
    {
        setValue(index, child.getId());

        children[index] = child;
    }

    void insertChild(final int index, final long key, final BTreeNode child)
    {
        final int rawChildPageCount = getRawChildPageCount();
        insertKey(index, key);
        insertValue(index, child.getId()); //TODO: store the page number of the child

        BTreeNode[] newChildren = new BTreeNode[rawChildPageCount + 1];
        copyWithGap(children, newChildren, rawChildPageCount, index);
        children = newChildren;
        children[index] = child;
    }

    int getRawChildPageCount()
    {
        return getKeyCount() + 1;
    }

    BTreeNode getChildAtIndex(final int index)
    {
        return children[index];
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKey(index, keyCount);
        removeValue(index, keyCount);

        removeChild(index);
    }

    private void removeChild(final int index)
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
    public BTreeNode copy(final Memory memoryForCopy)
    {
        MemoryCopy.copy(buffer, memoryForCopy);

        final BTreeNode[] copyChildren = new BTreeNode[children.length];
        System.arraycopy(children, 0, copyChildren, 0, children.length);

        return new BTreeNodeNonLeaf(getId(), memoryForCopy, numberOfKeys, numberOfValues, copyChildren, idSupplier);
    }

    @Override
    public boolean needRebalancing(int threshold)
    {
        return numberOfValues < 2;
    }

    @Override
    public BTreeNode split(final int at, final Memory memoryForNewNode)
    {
        final int keyCount = getKeyCount();
        if (keyCount <= 0)
        {
            return null;
        }

        final int aNumberOfValues = at + 1;
        final int bNumberOfKeys = numberOfKeys - aNumberOfValues;
        final int bNumberOfValues = numberOfValues - aNumberOfValues;

        final BTreeNode[] aChildren = new BTreeNode[aNumberOfValues];
        final BTreeNode[] bChildren = new BTreeNode[bNumberOfValues];
        System.arraycopy(children, 0, aChildren, 0, aNumberOfValues);
        System.arraycopy(children, aNumberOfValues, bChildren, 0, bNumberOfValues);
        children = aChildren;

        //TODO: allocate from other place
        final BTreeNodeNonLeaf bTreeNodeLeaf = new BTreeNodeNonLeaf(
                memoryForNewNode,
                bNumberOfKeys,
                bNumberOfValues,
                bChildren,
                idSupplier);
        bTreeNodeLeaf.updateNumberOfKeys(bTreeNodeLeaf.numberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bTreeNodeLeaf.numberOfValues);

        splitKeys(at, bNumberOfKeys, bTreeNodeLeaf);
        splitValues(aNumberOfValues, bNumberOfValues, bTreeNodeLeaf);

        return bTreeNodeLeaf;
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
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize
                    - gapIndex);
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
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize
                    - removeIndex - 1);
        }
    }
}
