package org.logdb.bbtree;

import java.util.Comparator;

public final class SearchUtils
{
    private SearchUtils()
    {
    }

    /**
     * <p>Tries to find a key in a previously sorted array.</p>
     *
     * <p>If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page, -(existingKeys + 1) if it's bigger
     * or somewhere in the middle.</p>
     *
     * <p>Note that this guarantees that the return value will be >= 0 if and only if
     * the key is found.</p>
     *
     * <p>See also Arrays.binarySearch.</p>
     *
     * @param key the key to find
     * @param numberOfKeys the number of total keys
     * @param keyIndexSupplier a functions that gets the key for a given index/position
     * @return the index in existing keys or negative
     */
    static int binarySearch(
            final long key,
            final int numberOfKeys,
            final KeyIndexSupplier<Long> keyIndexSupplier)
    {
        int low = 0;
        int high = numberOfKeys - 1;
        int index = high >>> 1;

        while (low <= high)
        {
            final long existingKey = keyIndexSupplier.getKey(index);
            final int compare = Long.compare(key, existingKey);
            if (compare > 0)
            {
                low = index + 1;
            }
            else if (compare < 0)
            {
                high = index - 1;
            }
            else
            {
                return index;
            }
            index = (low + high) >>> 1;
        }
        return -(low + 1);
    }

    /**
     * <p>Tries to find a key in a previously sorted array.</p>
     *
     * <p>If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page, -(existingKeys + 1) if it's bigger
     * or somewhere in the middle.</p>
     *
     * <p>Note that this guarantees that the return value will be >= 0 if and only if
     * the key is found.</p>
     *
     * <p>See also Arrays.binarySearch.</p>
     *
     * @param key the key to find
     * @param numberOfKeys the number of total keys
     * @param keyIndexSupplier a functions that gets the key for a given index/position
     * @return the index in existing keys or negative
     */
    static <T> int binarySearch(
            final T key,
            final int numberOfKeys,
            final KeyIndexSupplier<T> keyIndexSupplier,
            final Comparator<T> comparator)
    {
        int low = 0;
        int high = numberOfKeys - 1;
        int index = high >>> 1;

        while (low <= high)
        {
            final T existingKey = keyIndexSupplier.getKey(index);
            final int compare = comparator.compare(key, existingKey);
            if (compare > 0)
            {
                low = index + 1;
            }
            else if (compare < 0)
            {
                high = index - 1;
            }
            else
            {
                return index;
            }
            index = (low + high) >>> 1;
        }
        return -(low + 1);
    }

    /**
     * <p>Tries to find a key in a previously sorted array.</p>
     *
     * <p>If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page, -(existingKeys + 1) if it's bigger
     * or somewhere in the middle.</p>
     *
     * <p>Note that this guarantees that the return value will be >= 0 if and only if
     * the key is found.</p>
     *
     * <p>See also Arrays.binarySearch.</p>
     *
     * @param key the key to find
     * @param numberOfKeys the number of total keys
     * @param keyIndexSupplier a functions that gets the key for a given index/position
     * @return the index in existing keys or negative
     */
    public static long binarySearchLessOrEqual(
            final long key,
            final long numberOfKeys,
            final LongKeyIndexSupplier keyIndexSupplier)
    {
        long low = 0;
        long high = numberOfKeys - 1;
        long index = high >>> 1;

        while (low <= high)
        {
            final long existingKey = keyIndexSupplier.getKey(index);
            final int compare = Long.compare(key, existingKey);
            if (compare > 0)
            {
                low = index + 1;
            }
            else if (compare < 0)
            {
                high = index - 1;
            }
            else
            {
                return index;
            }
            index = (low + high) >> 1;
        }

        if (index < 0)
        {
            return InvalidBTreeValues.KEY_NOT_FOUND;
        }

        final long existingKey = keyIndexSupplier.getKey(index);
        final int compare = Long.compare(key, existingKey);

        return compare > 0 ? index : InvalidBTreeValues.KEY_NOT_FOUND;
    }

    interface KeyIndexSupplier<T>
    {
        T getKey(int index);
    }

    public interface LongKeyIndexSupplier
    {
        long getKey(long index);
    }
}
