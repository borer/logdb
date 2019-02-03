package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeNodeLeaf extends BTreeNodeAbstract
{
    private ByteBuffer[] values;

    public BTreeNodeLeaf()
    {
        this(new ByteBuffer[0], new ByteBuffer[0]);
    }

    public BTreeNodeLeaf(final ByteBuffer[] keys, final ByteBuffer[] values)
    {
        super(keys);
        this.values = values;
    }

    /**
     * Copy constructor
     */
    private BTreeNodeLeaf(
            final String id,
            final ByteBuffer[] keys,
            final ByteBuffer[] values)
    {
        super(id, keys);
        this.values = values;
    }

    @Override
    public ByteBuffer get(final ByteBuffer key)
    {
        final int index = SearchUtils.binarySearch(key, keys);

        if (index < 0)
        {
            return null;
        }

        return getValueAtIndex(index);
    }

    @Override
    public BTreeNode copy()
    {
        final ByteBuffer[] copyKeys = new ByteBuffer[keys.length];
        final ByteBuffer[] copyValues = new ByteBuffer[values.length];
        System.arraycopy(keys, 0, copyKeys, 0, keys.length);
        System.arraycopy(values, 0, copyValues, 0, values.length);

        return new BTreeNodeLeaf(getId(), keys, values);
    }

    @Override
    public BTreeNode split(final int at)
    {
        final int bSize = getKeyCount() - at;
        final ByteBuffer[] bKeys = splitKeys(at, bSize);
        final ByteBuffer[] bValues = new ByteBuffer[bSize];

        if (values != null)
        {
            final ByteBuffer[] aValues = new ByteBuffer[at];
            System.arraycopy(values, 0, aValues, 0, at);
            System.arraycopy(values, at, bValues, 0, bSize);
            values = aValues;
        }

        final BTreeNode bTreeNode = create(bKeys, bValues, null);

        return bTreeNode;
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
        removeValue(index, keyCount);
    }

    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        final int index = SearchUtils.binarySearch(key, keys);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            final int keyCount = getKeyCount();
            insertKey(absIndex, key);
            insertValue(absIndex, keyCount, value);
        }
        else
        {
            setValue(index, value);
        }
    }

    @Override
    public void print(StringBuilder printer)
    {
        final String id = getId();
        printer.append(String.format("\"%s\"", id));
        printer.append("[label = \"");

        for (int i = 0; i < keys.length; i++)
        {
            final String key = new String(keys[i].array());
            final String value = new String(values[i].array());
            printer.append(String.format(" <%s> |%s| ", key, value));
        }

        printer.append("\"];\n");
    }

    /**
     * Gets the value at index position.
     * @param index Index inside the btree leaf
     * @return The value or null if it doesn't exist
     */
    public ByteBuffer getValueAtIndex(final int index)
    {
        final ByteBuffer value = values[index];
        value.rewind();
        return value;
    }

    private void removeValue(final int index, final int keyCount)
    {
        final ByteBuffer[] newValues = new ByteBuffer[keyCount - 1];
        copyExcept(values, newValues, keyCount, index);
        values = newValues;
    }

    private void insertValue(final int index, final int keyCount, final ByteBuffer value)
    {
        final ByteBuffer[] newValues = new ByteBuffer[keyCount + 1];
        copyWithGap(values, newValues, keyCount, index);
        values = newValues;

        values[index] = value;
    }

    private ByteBuffer setValue(final int index, final ByteBuffer value)
    {
        values = values.clone();
        final ByteBuffer old = values[index];
        values[index] = value;

        return old;
    }
}
