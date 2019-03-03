package org.borer.logdb;

import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

public class BTreeNodeLeaf extends BTreeNodeAbstract
{
    protected ByteBuffer[] values;

    public BTreeNodeLeaf()
    {
        this(new ByteBuffer[0], new ByteBuffer[0], new IdSupplier());
    }

    public BTreeNodeLeaf(
            final ByteBuffer[] keys,
            final ByteBuffer[] values,
            final LongSupplier idSupplier)
    {
        super(keys, idSupplier);
        this.values = values;
    }

    /**
     * Copy constructor.
     */
    private BTreeNodeLeaf(
            final long id,
            final ByteBuffer[] keys,
            final ByteBuffer[] values,
            final LongSupplier idSupplier)
    {
        super(id, keys, idSupplier);
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

        return new BTreeNodeLeaf(getId(), copyKeys, copyValues, idSupplier);
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

        return new BTreeNodeLeaf(bKeys, bValues, idSupplier);
    }

    @Override
    public boolean needRebalancing(final int threshold)
    {
        return this.keys.length == 0 || this.keys.length < threshold;
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

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
        assert newValues.length >= 0
                : String.format("value size after removing index %d was %d", index, newValues.length);
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
