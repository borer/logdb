package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeNode extends BTreeAbstract
{
    private BTree[] children;

    public BTreeNode()
    {
        super(new ByteBuffer[0]);
        this.children = new BTree[0];
    }

    public BTreeNode(ByteBuffer[] keys, BTree[] children)
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
    public BTree split(final int at)
    {
        final int b = getKeyCount() - at;
        final ByteBuffer[] bKeys = splitKeys(at, b - 1);
        final BTree[] aChildren = new BTree[at + 1];
        final BTree[] bChildren = new BTree[b];
        System.arraycopy(children, 0, aChildren, 0, at + 1);
        System.arraycopy(children, at + 1, bChildren, 0, b);
        children = aChildren;

        return create(bKeys, null, bChildren);
    }
}
