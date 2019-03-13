package org.borer.logdb;

import java.nio.ByteBuffer;

final class SearchUtils
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
     * @param existingKeys sorted array that is used for the search
     * @return the index in existing keys or negative
     */
    static int binarySearch(final ByteBuffer key, final ByteBuffer[] existingKeys)
    {
        int low = 0;
        int high = existingKeys.length - 1;
        int index = high >>> 1;

        while (low <= high)
        {
            final ByteBuffer existingKey = existingKeys[index];
            key.rewind();
            existingKey.rewind();
            final int compare = key.compareTo(existingKey);
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
}
