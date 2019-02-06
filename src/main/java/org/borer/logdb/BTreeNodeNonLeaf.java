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

    public BTreeNodeNonLeaf(final ByteBuffer[] keys, final BTreeNode[] children)
    {
        super(keys);
        this.children = children;
    }

    /**
     * Copy constructor
     */
    private BTreeNodeNonLeaf(
            final String id,
            final ByteBuffer[] keys,
            final BTreeNode[] children)
    {
        super(id, keys);
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

    BTreeNode getChildPage(final int index)
    {
        return children[index];
    }

    @Override
    public void remove(final ByteBuffer key)
    {
        final int index = SearchUtils.binarySearch(key, keys);
        if (index < 0)
        {
            return;
        }

        final int keyCount = getKeyCount();
        removeKey(index, keyCount);
        removeChild(index, keyCount);
    }

    private void removeChild(final int index, final int keyCount)
    {
        final BTreeNode[] newChildren = new BTreeNode[keyCount - 1];
        copyExcept(children, newChildren, keyCount, index);
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

        return new BTreeNodeNonLeaf(getId(), copyKeys, copyChildren);
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

        return new BTreeNodeNonLeaf(bKeys, bChildren);
    }

    @Override
    public void print(final StringBuilder printer)
    {
        final String id = getId();
        final String lastChildId = children[keys.length].getId();

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        for (ByteBuffer key : keys)
        {
            final String keyLabel = new String(key.array());
            printer.append(String.format(" <%s> |%s| ", keyLabel, keyLabel));
        }
        printer.append(" <lastChild> |Ls ");
        printer.append("\"];\n");

        for (int i = 0; i < keys.length; i++)
        {
            final String keyLabel = new String(keys[i].array());
            final String childId = children[i].getId();
            printer.append(String.format("\"%s\":%s -> \"%s\"", id, keyLabel, childId));
            printer.append("\n");
        }

        printer.append(String.format("\"%s\":lastChild -> \"%s\"", id, lastChildId));
        printer.append("\n");

        for (final BTreeNode child : children)
        {
            child.print(printer);
        }
    }
}
