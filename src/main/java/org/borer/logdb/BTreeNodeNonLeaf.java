package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeNodeNonLeaf extends BTreeNodeAbstract
{
    private BTreeNode[] children;

    public BTreeNodeNonLeaf()
    {
        super(new ByteBuffer[0], null, null);
        this.children = new BTreeNode[1]; //there is always one child at least
    }

    public BTreeNodeNonLeaf(
            final ByteBuffer[] keys,
            final BTreeNode[] children,
            final BTreeNode leftSibling,
            final BTreeNode rightSibling)
    {
        super(keys, leftSibling, rightSibling);
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

    private int getRawChildPageCount()
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
    public BTreeNode split(final int at)
    {
        final int b = getKeyCount() - at;
        final ByteBuffer[] bKeys = splitKeys(at, b - 1);
        final BTreeNode[] aChildren = new BTreeNode[at + 1];
        final BTreeNode[] bChildren = new BTreeNode[b];
        System.arraycopy(children, 0, aChildren, 0, at + 1);
        System.arraycopy(children, at + 1, bChildren, 0, b);
        children = aChildren;

        final BTreeNode bTreeNode = create(
                bKeys,
                null,
                bChildren,
                this,
                this.rightSibling);

        if (this.rightSibling != null)
        {
            this.rightSibling.setLeftSibling(bTreeNode);
        }

        this.setRightSibling(bTreeNode);

        return bTreeNode;
    }

    @Override
    public void print(final StringBuilder printer)
    {
        final String id = getId();
        final String lastChildId = children[keys.length].getId();

        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        if (this.leftSibling != null)
        {
            printer.append(" <leftSibling> L| ");
        }

        for (ByteBuffer key : keys)
        {
            final String keyLabel = new String(key.array());
            printer.append(String.format(" <%s> |%s| ", keyLabel, keyLabel));
        }
        printer.append(" <lastChild> Ls| ");

        if (this.rightSibling != null)
        {
            printer.append(" <rightSibling> R| ");
        }

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

        if (this.leftSibling != null)
        {
            printer.append(String.format(LEFT_SIBLING_PRINTER_FORMAT, id, this.leftSibling.getId()));
            printer.append("\n");
        }

        if (this.rightSibling != null)
        {
            printer.append(String.format(RIGHT_SIBLING_PRINTER_FORMAT, id, this.rightSibling.getId()));
            printer.append("\n");
        }

        for (final BTreeNode child : children)
        {
            child.print(printer);
        }
    }
}
