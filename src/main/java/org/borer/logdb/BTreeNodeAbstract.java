package org.borer.logdb;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

abstract class BTreeNodeAbstract implements BTreeNode
{
    private static AtomicLong idCounter = new AtomicLong();

    private final String id;

    ByteBuffer[] keys;

    BTreeNodeAbstract(
            final ByteBuffer[] keys)
    {
        this(String.valueOf(idCounter.getAndIncrement()), keys);
    }

    /**
     * Copy constructor
     */
    BTreeNodeAbstract(final String id, final ByteBuffer[] keys)
    {
        this.id = id;
        this.keys = keys;
    }

    @Override
    public String getId()
    {
        //TODO: use page offset
        return id;
    }

    @Override
    public int getKeyCount()
    {
        return keys.length;
    }

    /**
     * Returns a key at the given index.
     * @param index has to be between 0...getKeyCount()
     * @return the key
     */
    @Override
    public ByteBuffer getKey(final int index)
    {
        return keys[index];
    }

    /**
     * Gets the key at index position.
     * @param index Index inside the btree leaf
     * @return The key or null if it doesn't exist
     */
    public ByteBuffer getKeyAtIndex(final int index)
    {
        return keys[index];
    }

    void insertKey(final int index, final ByteBuffer key)
    {
        final int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;

        final ByteBuffer[] newKeys = new ByteBuffer[keyCount + 1];
        copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;
    }

    void removeKey(final int index, final int keyCount)
    {
        final ByteBuffer[] newKeys = new ByteBuffer[keyCount - 1];
        copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    final ByteBuffer[] splitKeys(int aCount, int bCount)
    {
        assert aCount + bCount <= getKeyCount();
        ByteBuffer[] aKeys = new ByteBuffer[aCount];
        ByteBuffer[] bKeys = new ByteBuffer[bCount];
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param gapIndex the index of the gap
     */
    static void copyWithGap(
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
    static void copyExcept(
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
