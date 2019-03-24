package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;

import java.util.function.LongSupplier;

import static org.borer.logdb.Config.PAGE_SIZE_BYTES;

abstract class BTreeNodeAbstract implements BTreeNode
{
    private static final int PAGE_START_OFFSET = 0;

    private static final int PAGE_HEADER_OFFSET = PAGE_START_OFFSET;
    private static final int PAGE_HEADER_SIZE = Long.BYTES;

    private static final int NUMBER_OF_KEY_OFFSET = PAGE_HEADER_OFFSET + PAGE_HEADER_SIZE;
    private static final int NUMBER_OF_KEY_SIZE = Integer.BYTES;

    private static final int NUMBER_OF_VALUES_OFFSET = PAGE_START_OFFSET + PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE;
    private static final int NUMBER_OF_VALUES_SIZE = Integer.BYTES;

    private static final int HEADER_SIZE_BYTES = PAGE_HEADER_SIZE + PAGE_HEADER_SIZE + NUMBER_OF_KEY_SIZE + NUMBER_OF_VALUES_SIZE;
    private static final int KEY_START_OFFSET = HEADER_SIZE_BYTES;

    private static final int KEY_SIZE = Long.BYTES;
    private static final int VALUE_SIZE = Long.BYTES;

    private long id;
    private long freeSizeLeftBytes;

    long pageNumber;
    final Memory buffer;
    int numberOfKeys;
    int numberOfValues;
    boolean isDirty;

    final LongSupplier idSupplier;

    /**
     * Index Page layout :
     * ------------------------------------------ 0
     * |                 Header                 |
     * ------------------------------------------ 8
     * |   Number of keys  |  Number of values  |
     * ------------------------------------------ 16
     * |                   Key1                 |
     * ------------------------------------------ 24
     * |                   Key2                 |
     * ------------------------------------------ 32
     * |                  ......                |
     * ------------------------------------------ number of keys * 8 = N
     * |                  ......                |
     * ------------------------------------------ 4072
     * |                   Value3               |
     * ------------------------------------------ 4080
     * |                   Value2               |
     * ------------------------------------------ 4088
     * |                   Value1               |
     * ------------------------------------------ 4096 (end of page)
     *
     * The keys are always 8 bytes.
     *
     * For leaf nodes the values are pointers in the leaf nodes to the page that has the element
     * For internal index nodes the values are pointer in the index file to the page that has the child
     * Values are always 8 bytes (long)
     *
     *
     * @param buffer the buffer used as a content for this node
     * @param idSupplier the id generator used to assign id to this node and splitted nodes
     */
    BTreeNodeAbstract(final Memory buffer,
                      final int numberOfKeys,
                      final int numberOfValues,
                      final LongSupplier idSupplier)
    {
        this(idSupplier.getAsLong(), buffer, numberOfKeys, numberOfValues, idSupplier);
    }

    /**
     * Copy constructor.
     */
    BTreeNodeAbstract(final long id,
                      final Memory buffer,
                      final int numberOfKeys,
                      final int numberOfValues,
                      final LongSupplier idSupplier)
    {
        this.id = id;
        this.pageNumber = -1;
        this.buffer = buffer;
        this.numberOfKeys = numberOfKeys;
        this.numberOfValues = numberOfValues;
        this.idSupplier = idSupplier;
        final int usedBytes = (numberOfKeys * KEY_SIZE) + (numberOfValues * VALUE_SIZE) + HEADER_SIZE_BYTES;
        this.freeSizeLeftBytes = PAGE_SIZE_BYTES - usedBytes;
        this.isDirty = true;
    }

    //TODO: call this function after creation of the node ?? or before ?? or during??
    protected void setNodePage(final BtreeNodeType type)
    {
        buffer.putByte(0, type.getType());
    }

    @Override
    public long getId()
    {
        return pageNumber != -1 ? pageNumber : id;
    }

    @Override
    public int getKeyCount()
    {
        return numberOfKeys;
    }

    @Override
    public long getKey(final int index)
    {
        return buffer.getLong(getKeyIndexOffset(index));
    }

    private static long getKeyIndexOffset(final int index)
    {
        return KEY_START_OFFSET + (index * KEY_SIZE);
    }

    private static long getValueIndexOffset(final int index)
    {
        return PAGE_SIZE_BYTES - ((index + 1) * VALUE_SIZE);
    }

    void insertKey(final int index, final long key)
    {
        final int keyCount = getKeyCount();
        assert index <= keyCount
                : String.format("index to insert %d > node key cound %d ", index, keyCount);

        copyKeysWithGap(index);

        buffer.putLong(getKeyIndexOffset(index), key);

        numberOfKeys++;
        updateNumberOfKeys(numberOfKeys);

        freeSizeLeftBytes -= KEY_SIZE;
    }

    void removeKey(final int index, final int keyCount)
    {
        assert (keyCount - 1) >= 0
                : String.format("key size after removing index %d was %d", index, keyCount - 1);
        copyKeysExcept(index);

        numberOfKeys--;
        updateNumberOfKeys(numberOfKeys);

        freeSizeLeftBytes += KEY_SIZE;
    }

    void updateNumberOfKeys(final int numberOfKeys)
    {
        buffer.putInt(NUMBER_OF_KEY_OFFSET, numberOfKeys);
    }

    void updateNumberOfValues(final int numberOfValues)
    {
        buffer.putInt(NUMBER_OF_VALUES_OFFSET, numberOfValues);
    }

    /**
     * Gets the value at index position.
     * @param index Index inside the btree leaf
     * @return The value or null if it doesn't exist
     */
    long getValue(final int index)
    {
        return buffer.getLong(getValueIndexOffset(index));
    }

    void removeValue(final int index, final int keyCount)
    {
        assert keyCount >= 0
                : String.format("value size after removing index %d was %d", index, keyCount - 1);
        copyValuesExcept(index);

        numberOfValues--;
        updateNumberOfValues(numberOfValues);

        freeSizeLeftBytes += VALUE_SIZE;
    }

    void insertValue(final int index, final long value)
    {
        copyValuesWithGap(index);
        setValue(index, value);

        numberOfValues++;
        updateNumberOfValues(numberOfValues);

        freeSizeLeftBytes -= VALUE_SIZE;
    }

    long setValue(final int index, final long value)
    {
        final long valueIndexOffset = getValueIndexOffset(index);
        final long oldValue = buffer.getLong(valueIndexOffset);
        buffer.putLong(valueIndexOffset, value);

        return oldValue;
    }

    void splitKeys(final int aNumberOfKeys, final int bNumberOfKeys, final BTreeNodeAbstract bNode)
    {
        //copy keys
        final byte[] bKeysBuffer = new byte[bNumberOfKeys * KEY_SIZE];
        buffer.getBytes(getKeyIndexOffset(numberOfKeys - bNumberOfKeys), bKeysBuffer);
        bNode.buffer.putBytes(getKeyIndexOffset(0), bKeysBuffer);

        freeSizeLeftBytes += (numberOfKeys - aNumberOfKeys) * KEY_SIZE;
        numberOfKeys = aNumberOfKeys;
        updateNumberOfKeys(numberOfKeys);
    }

    void splitValues(final int aNumberOfValues, int bNumberOfValues, final BTreeNodeAbstract bNode)
    {
        //copy values
        final byte[] bValuesBuffer = new byte[bNumberOfValues * VALUE_SIZE];
        buffer.getBytes(getValueIndexOffset(aNumberOfValues + bNumberOfValues - 1), bValuesBuffer);
        bNode.buffer.putBytes(getValueIndexOffset(bNumberOfValues - 1), bValuesBuffer);

        freeSizeLeftBytes += (numberOfValues - aNumberOfValues) * VALUE_SIZE;
        numberOfValues = aNumberOfValues;
        updateNumberOfValues(numberOfValues);
    }

    protected void setDirty()
    {
        isDirty = true;
    }

    @Override
    public boolean isDirty()
    {
        return isDirty;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param gapIndex the index of the gap
     */
    private void copyKeysWithGap(final int gapIndex)
    {
        if (gapIndex < numberOfKeys)
        {
            final int elementsToMove = numberOfKeys - gapIndex;
            final long oldKeyIndexOffset = getKeyIndexOffset(gapIndex);
            final long newKeyIndexOffset = getKeyIndexOffset(gapIndex + 1);
            byte[] bufferForElementsToMove = new byte[elementsToMove * KEY_SIZE];

            buffer.getBytes(oldKeyIndexOffset, bufferForElementsToMove);
            buffer.putBytes(newKeyIndexOffset, bufferForElementsToMove);
        }
    }

    private void copyKeysExcept(final int removeIndex)
    {
        if (numberOfKeys > 0 && removeIndex < numberOfKeys)
        {
            final int elementsToMove = numberOfKeys - removeIndex;
            final long oldKeyIndexOffset = getKeyIndexOffset(removeIndex);
            final long newKeyIndexOffset = getKeyIndexOffset(removeIndex + 1);
            byte[] bufferForElementsToMove = new byte[elementsToMove * KEY_SIZE];

            buffer.getBytes(newKeyIndexOffset, bufferForElementsToMove);
            buffer.putBytes(oldKeyIndexOffset, bufferForElementsToMove);
        }
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param gapIndex the index of the gap
     */
    private void copyValuesWithGap(final int gapIndex)
    {
        if (gapIndex < numberOfValues)
        {
            final int elementsToMove = numberOfValues - gapIndex;
            final long oldValueIndexOffset = getValueIndexOffset(numberOfValues - 1);
            final long newValueIndexOffset = getValueIndexOffset(numberOfValues);
            byte[] bufferForElementsToMove = new byte[elementsToMove * VALUE_SIZE];

            buffer.getBytes(oldValueIndexOffset, bufferForElementsToMove);
            buffer.putBytes(newValueIndexOffset, bufferForElementsToMove);
        }
    }

    private void copyValuesExcept(final int removeIndex)
    {
        if (numberOfValues > 0 && removeIndex < (numberOfValues - 1))
        {
            final int elementsToMove = numberOfValues - removeIndex;
            final long oldValueIndexOffset = getValueIndexOffset(numberOfValues);
            final long newValueIndexOffset = getValueIndexOffset(numberOfValues - 1);
            byte[] bufferForElementsToMove = new byte[elementsToMove * VALUE_SIZE];

            buffer.getBytes(oldValueIndexOffset, bufferForElementsToMove);
            buffer.putBytes(newValueIndexOffset, bufferForElementsToMove);
        }
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
     * @return the index in existing keys or negative
     */
    int binarySearch(final long key)
    {
        int low = 0;
        int high = numberOfKeys - 1;
        int index = high >>> 1;

        while (low <= high)
        {
            final long existingKey = getKey(index);
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

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder();

        contentBuilder.append(" keys : ");
        for (int i = 0; i < numberOfKeys; i++)
        {
            contentBuilder.append(getKey(i));
            if (i + 1 != numberOfKeys)
            {
                contentBuilder.append(",");
            }
        }
        contentBuilder.append(" values : ");
        for (int i = 0; i < numberOfValues; i++)
        {
            contentBuilder.append(getValue(i));
            if (i + 1 != numberOfValues)
            {
                contentBuilder.append(",");
            }
        }

        return String.format("BTreeNodeLeaf{ %s }", contentBuilder.toString());
    }
}
