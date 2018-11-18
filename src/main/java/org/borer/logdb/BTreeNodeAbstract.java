package org.borer.logdb;

import java.nio.ByteBuffer;

abstract class BTreeNodeAbstract implements BTreeNode
{
    ByteBuffer[] keys;

    BTreeNodeAbstract(ByteBuffer[] keys)
    {
        this.keys = keys;
    }

    @Override
    public void print(StringBuilder printer, String label)
    {
        if ("root".equals(label))
        {
            printer.append("digraph g {\n");
            printer.append("node [shape = record,height=.1];\n");
        }

        printNode(printer, label);

        if ("root".equals(label))
        {
            printer.append("}\n");
        }
    }

    public abstract void printNode(StringBuilder printer, String label);

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     *
     * <p>If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or -1
     */
    protected int binarySearch(final ByteBuffer key)
    {
        int low = 0;
        int high = keys.length - 1;
        int index = high >>> 1;

        final ByteBuffer[] existingKeys = keys;
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

    /**
     * Get the number of entries in the leaf.
     * @return the number of entries.
     */
    public int getKeyCount()
    {
        return keys.length;
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

    void insertKey(final int index, final int keyCount, final ByteBuffer key)
    {
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
     * Create a new btree. The arrays are not cloned.
     *
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @return the page
     */
    static BTreeNode create(
            ByteBuffer[] keys,
            ByteBuffer[] values,
            BTreeNode[] children)
    {
        assert keys != null;
        return (children == null)
                ? new BTreeNodeLeaf(keys, values)
                : new BTreeNodeNonLeaf(keys, children);
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
            final Object src,
            final Object dst,
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
            final Object src,
            final Object dst,
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
