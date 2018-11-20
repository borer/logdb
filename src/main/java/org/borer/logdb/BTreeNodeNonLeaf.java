package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    private BTreeNode[] children;

    public BTreeNodeNonLeaf()
    {
        super(new ByteBuffer[0]);
        this.children = new BTreeNode[1]; //there is always one child at least
    }

    public BTreeNodeNonLeaf(ByteBuffer[] keys, BTreeNode[] children)
    {
        super(keys);
        this.children = children;
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        children[index].insert(key, value);
    }

    public void setChildren(final int index, final BTreeNode child)
    {
        children[index] = child;
    }

    public void insertChild(final ByteBuffer key, final BTreeNode child)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            final int absIndex = -index - 1;
            final int keyCount = getKeyCount();
            insertKey(absIndex, keyCount, key);
            insertChild(absIndex, keyCount + 1, child);
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
        final int index = binarySearch(key) +  1;
        if (index < 0)
        {
            return;
        }

        children[index].remove(key);
    }

    @Override
    public ByteBuffer get(final ByteBuffer key)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return children[index].get(key);
    }

    /**
     * Splits the current node into 2 nodes.
     * Current node with all the keys from 0...at-1 and a new one from at+1...end.
     * @param at the key index that we are going to split by
     * @return a new node containing from at+1...end children
     */
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

    @Override
    public void printNode(StringBuilder printer, final String label)
    {
        printer.append(label);
        printer.append("[label = \"");
        for (ByteBuffer key : keys)
        {
            final String keyLabel = new String(key.array());
            printer.append(String.format("<%s> |%s|", keyLabel, keyLabel));
        }
        printer.append("\"];\n");

        for (ByteBuffer key : keys)
        {
            final String keyLabel = new String(key.array());
            printer.append(String.format("\"%s\":%s -> \"%s\"", label, keyLabel, keyLabel));
            printer.append("\n");
        }

        for (int i = 0; i < keys.length; i++)
        {
            final String key = new String(keys[i].array());
            children[i].print(printer, key);
        }
    }
}
