package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    private BTreeNode[] children;

    public BTreeNodeNonLeaf()
    {
        super(new ByteBuffer[0]);
        this.children = new BTreeNode[0];
    }

    public BTreeNodeNonLeaf(ByteBuffer[] keys, BTreeNode[] children)
    {
        super(keys);
        this.children = children;
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return;
        }

        children[index].insert(key, value);
    }

    @Override
    public void insertChild(final ByteBuffer key, final BTreeNode child)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            final int keyCount = getKeyCount();
            insertKey(absIndex, keyCount, key);
            insertChild(absIndex, keyCount, child);
        }
    }

    private void insertChild(final int index, final int keyCount, final BTreeNode child)
    {
        BTreeNode[] newChildren = new BTreeNode[keyCount + 1];
        copyWithGap(children, newChildren, keyCount, index);
        children = newChildren;
        children[index] = child;
    }

    @Override
    public void remove(final ByteBuffer key)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return;
        }

        children[index].remove(key);
    }

    @Override
    public ByteBuffer get(final ByteBuffer key)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return null;
        }

        return children[index].get(key);
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

        return create(bKeys, null, bChildren);
    }
}
