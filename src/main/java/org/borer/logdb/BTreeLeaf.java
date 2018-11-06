package org.borer.logdb;

import java.nio.ByteBuffer;

public class BTreeLeaf extends BTreeAbstract
{
    private ByteBuffer[] values;

    public BTreeLeaf()
    {
        super(new ByteBuffer[0]);
        values = new ByteBuffer[0];
    }

    public BTreeLeaf(final ByteBuffer[] keys, final ByteBuffer[] values)
    {
        super(keys);
        this.values = values;
    }

    /**
     * Get the value corresponding to the key.
     * @param key key to search for
     * @return the value corresponding to that key or null if not found
     */
    @Override
    public ByteBuffer get(final ByteBuffer key)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return null;
        }

        return getValueAtIndex(index);
    }

    @Override
    public BTree split(final int at)
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

        return create(bKeys, bValues, null);
    }

    /**
     * Remove the key and value.
     *
     * @param key the key to remove
     */
    @Override
    public void remove(final ByteBuffer key)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return;
        }

        final int keyCount = getKeyCount();
        removeKey(index, keyCount);
        removeValue(index, keyCount);
    }

    /**
     * Inserts key/value pair in the current leaf.
     * If the key already exits, its value is replaced.
     * @param key Key that identifies the value
     * @param value Value to persist
     */
    @Override
    public void insert(final ByteBuffer key, final ByteBuffer value)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            final int keyCount = getKeyCount();
            insertKey(absIndex, keyCount, key);
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
