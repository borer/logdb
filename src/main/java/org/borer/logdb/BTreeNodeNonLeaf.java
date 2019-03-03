package org.borer.logdb;

import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    protected BTreeNode[] children;

    public BTreeNodeNonLeaf()
    {
        super(new ByteBuffer[0], new IdSupplier());
        this.children = new BTreeNode[1]; //there is always one child at least
    }

    public BTreeNodeNonLeaf(
            final ByteBuffer[] keys,
            final BTreeNode[] children,
            final LongSupplier idSupplier)
    {
        super(keys, idSupplier);
        this.children = children;
    }

    /**
     * Copy constructor.
     */
    private BTreeNodeNonLeaf(
            final long id,
            final ByteBuffer[] keys,
            final BTreeNode[] children,
            final LongSupplier idSupplier)
    {
        super(id, keys, idSupplier);
        this.children = children;
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        int index = SearchUtils.binarySearch(key, keys) + 1;
        if (index < 0)
        {
            index = -index;
        }

        children[index].insert(key, value);
    }

    void setChild(final int index, final BTreeNode child)
    {
        children[index] = child;
    }

    void insertChild(final int index, final ByteBuffer key, final BTreeNode child)
    {
        final int rawChildPageCount = getRawChildPageCount();
        insertKey(index, key);

        BTreeNode[] newChildren = new BTreeNode[rawChildPageCount + 1];
        copyWithGap(children, newChildren, rawChildPageCount, index);
        children = newChildren;
        children[index] = child;
    }

    protected int getRawChildPageCount()
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
    public ByteBuffer get(final ByteBuffer key)
    {
        int index = SearchUtils.binarySearch(key, keys) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return children[index].get(key);
    }

    @Override
    public BTreeNode copy()
    {
        final ByteBuffer[] copyKeys = new ByteBuffer[keys.length];
        final BTreeNode[] copyChildren = new BTreeNode[children.length];
        System.arraycopy(keys, 0, copyKeys, 0, keys.length);
        System.arraycopy(children, 0, copyChildren, 0, children.length);

        return new BTreeNodeNonLeaf(getId(), copyKeys, copyChildren, idSupplier);
    }

    @Override
    public boolean needRebalancing(int threshold)
    {
        return this.children.length < 2;
    }

    @Override
    public BTreeNode split(final int at)
    {
        final int b = getKeyCount() - at;
        final ByteBuffer[] bKeys = splitKeys(at, b - 1);
        final BTreeNode[] aChildren = new BTreeNode[at + 1];
        final BTreeNode[] bChildren = new BTreeNode[b];
        System.arraycopy(children, 0, aChildren, 0, at + 1);
        System.arraycopy(children, at + 1, bChildren, 0, b);
        children = aChildren;

        return new BTreeNodeNonLeaf(bKeys, bChildren, idSupplier);
    }
}
