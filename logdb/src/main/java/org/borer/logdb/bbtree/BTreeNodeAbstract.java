package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;

import java.util.Objects;

import static org.borer.logdb.Config.PAGE_SIZE_BYTES;
import static org.borer.logdb.bbtree.BTreeNodePage.HEADER_SIZE_BYTES;
import static org.borer.logdb.bbtree.BTreeNodePage.KEY_SIZE;
import static org.borer.logdb.bbtree.BTreeNodePage.KEY_START_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.NUMBER_OF_KEY_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.NUMBER_OF_VALUES_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_IS_ROOT_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_PREV_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_TIMESTAMP_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_TYPE_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_VERSION_OFFSET;
import static org.borer.logdb.bbtree.BTreeNodePage.VALUE_SIZE;

abstract class BTreeNodeAbstract implements BTreeNode
{
    private long freeSizeLeftBytes;

    long pageNumber;
    int numberOfKeys;
    int numberOfValues;
    int numberOfLogKeyValues;
    boolean isDirty;

    final Memory buffer;

    /**
     * Load constructor.
     *
     * @param memory the memory to load from
     */
    BTreeNodeAbstract(final long pageNumber, final Memory memory)
    {
        this(pageNumber,
                memory,
                memory.getInt(NUMBER_OF_KEY_OFFSET),
                memory.getInt(NUMBER_OF_VALUES_OFFSET));
    }

    /**
     * Copy/Split constructor.
     *
     * @param pageNumber     the page number of this node or an id generated for not yet persisted nodes
     * @param buffer         the buffer used as a content for this node
     * @param numberOfKeys   number of keys in this node
     * @param numberOfValues number of values in this node
     */
    BTreeNodeAbstract(final long pageNumber,
                      final Memory buffer,
                      final int numberOfKeys,
                      final int numberOfValues)
    {
        this.pageNumber = pageNumber;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.numberOfKeys = numberOfKeys;
        this.numberOfValues = numberOfValues;
        this.freeSizeLeftBytes = calculateFreeSpaceLeft();
        this.isDirty = true;
    }

    void preCommit(final boolean isRoot, final long previousRootPageNumber, final long timestamp, final long version)
    {
        if (isRoot)
        {
            preCommitRoot(previousRootPageNumber, timestamp, version);
        }
        else
        {
            preCommit();
        }
    }

    private void preCommit()
    {
        setNodePageType(getNodeType());
        setRootFlag(false);
        isDirty = false;
    }

    private void preCommitRoot(final long previousRootPageNumber, final long timestamp, final long version)
    {
        setNodePageType(getNodeType());
        setRootFlag(true);
        setPreviousRoot(previousRootPageNumber);
        setTimestamp(timestamp);
        setVersion(version);
        isDirty = false;
    }

    private void setNodePageType(final BtreeNodeType type)
    {
        buffer.putByte(PAGE_TYPE_OFFSET, type.getType());
    }

    void setRootFlag(final boolean isRoot)
    {
        buffer.putByte(PAGE_IS_ROOT_OFFSET, (byte) (isRoot ? 1 : 0));
    }

    public boolean isRoot()
    {
        return buffer.getByte(PAGE_IS_ROOT_OFFSET) == 1;
    }

    void setTimestamp(final long timestamp)
    {
        buffer.putLong(PAGE_TIMESTAMP_OFFSET, timestamp);
    }

    public long getTimestamp()
    {
        return buffer.getLong(PAGE_TIMESTAMP_OFFSET);
    }

    void setVersion(final long version)
    {
        buffer.putLong(PAGE_VERSION_OFFSET, version);
    }

    public long getVersion()
    {
        return buffer.getLong(PAGE_VERSION_OFFSET);
    }

    void setPreviousRoot(final long previousRootPageNumber)
    {
        buffer.putLong(PAGE_PREV_OFFSET, previousRootPageNumber);
    }

    public long getPreviousRoot()
    {
        return buffer.getLong(PAGE_PREV_OFFSET);
    }

    @Override
    public long getPageNumber()
    {
        return pageNumber;
    }

    public void initNodeFromBuffer()
    {
        numberOfKeys = buffer.getInt(NUMBER_OF_KEY_OFFSET);
        numberOfValues = buffer.getInt(NUMBER_OF_VALUES_OFFSET);

        if (getNodeType() == BtreeNodeType.NonLeaf)
        {
            numberOfLogKeyValues = buffer.getInt(PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET);
        }
        else
        {
            numberOfLogKeyValues = 0;
        }

        freeSizeLeftBytes = calculateFreeSpaceLeft();
    }

    private long calculateFreeSpaceLeft()
    {
        final int usedBytes = (numberOfKeys * KEY_SIZE) + (numberOfValues * VALUE_SIZE) + HEADER_SIZE_BYTES;
        return PAGE_SIZE_BYTES - usedBytes;
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
        return KEY_START_OFFSET + (index * (KEY_SIZE + VALUE_SIZE));
    }

    private static long getValueIndexOffsetNew(final int index)
    {
        return getKeyIndexOffset(index) + KEY_SIZE;
    }

    long getLogKey(final int index)
    {
        return buffer.getLong(getLogKeyIndexOffset(index));
    }

    long getLogValueFromBuffer(final int index)
    {
        return buffer.getLong(getLogValueIndexOffset(index));
    }

    private static long getLogKeyIndexOffset(final int index)
    {
        return PAGE_SIZE_BYTES - ((index + 1) * (KEY_SIZE + VALUE_SIZE));
    }

    private static long getLogValueIndexOffset(final int index)
    {
        return getLogKeyIndexOffset(index) + KEY_SIZE;
    }

    void insertKeyAndValue(final int index, final long key, final long value)
    {
        final int keyCount = getKeyCount();
        assert index <= keyCount
                : String.format("index to insert %d > node key cound %d ", index, keyCount);

        copyKeyValuesWithGap(index);

        buffer.putLong(getKeyIndexOffset(index), key);
        buffer.putLong(getValueIndexOffsetNew(index), value);

        numberOfKeys++;
        updateNumberOfKeys(numberOfKeys);
        numberOfValues++;
        updateNumberOfValues(numberOfValues);

        freeSizeLeftBytes -= KEY_SIZE + VALUE_SIZE;
    }

    void removeKeyAndValue(final int index, final int keyCount)
    {
        assert (keyCount - 1) >= 0
                : String.format("key size after removing index %d was %d", index, keyCount - 1);
        copyKeyValuesExcept(index);

        numberOfKeys--;
        updateNumberOfKeys(numberOfKeys);
        numberOfValues--;
        updateNumberOfValues(numberOfValues);

        freeSizeLeftBytes += KEY_SIZE + VALUE_SIZE;
    }

    void updateNumberOfKeys(final int numberOfKeys)
    {
        this.numberOfKeys = numberOfKeys;
        buffer.putInt(NUMBER_OF_KEY_OFFSET, numberOfKeys);
    }

    void updateNumberOfValues(final int numberOfValues)
    {
        this.numberOfValues = numberOfValues;
        buffer.putInt(NUMBER_OF_VALUES_OFFSET, numberOfValues);
    }

    void updateNumberOfLogKeyValues(final int numberOfLogKeyValues)
    {
        this.numberOfLogKeyValues = numberOfLogKeyValues;
        buffer.putInt(PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET, numberOfLogKeyValues);
    }

    /**
     * Gets the value at index position.
     *
     * @param index Index inside the btree leaf
     * @return The value or null if it doesn't exist
     */
    public long getValue(final int index)
    {
        return buffer.getLong(getValueIndexOffsetNew(index));
    }

    public long getLogValue(final long key)
    {
        final int index = logBinarySearch(key);
        if (index < 0)
        {
            return -1;
        }

        return getLogValueFromBuffer(index);
    }

    void removeLogKeyValue(final int index)
    {
        copyLogKeyValuesExcept(index);

        numberOfLogKeyValues--;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        freeSizeLeftBytes += KEY_SIZE + VALUE_SIZE;
    }

    void insertLogKeyValue(final int index, final long key, final long value)
    {
        copyLogKeyValuesWithGap(index);
        setLogKeyValue(index, key, value);

        numberOfLogKeyValues++;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        freeSizeLeftBytes -= KEY_SIZE + VALUE_SIZE;
    }

    void setLogKeyValue(final int index, final long key, final long value)
    {
        buffer.putLong(getLogKeyIndexOffset(index), key);
        buffer.putLong(getLogValueIndexOffset(index), value);
    }

    long setValue(final int index, final long value)
    {
        final long valueIndexOffset = getValueIndexOffsetNew(index);
        final long oldValue = buffer.getLong(valueIndexOffset);
        buffer.putLong(valueIndexOffset, value);

        return oldValue;
    }

    void splitKeysAndValues(
            final int aNumberOfKeys,
            final int bNumberOfKeys,
            final BTreeNodeAbstract bNode)
    {
        //copy keys
        final int extraValues = numberOfValues - numberOfKeys;
        final int lengthOfSplit = (bNumberOfKeys + extraValues) * (KEY_SIZE + VALUE_SIZE);
        final byte[] bKeyValueBuffer = new byte[lengthOfSplit];
        buffer.getBytes(getKeyIndexOffset(numberOfKeys - bNumberOfKeys), bKeyValueBuffer);
        bNode.buffer.putBytes(getKeyIndexOffset(0), bKeyValueBuffer);

        freeSizeLeftBytes += (numberOfKeys - aNumberOfKeys) * (KEY_SIZE + VALUE_SIZE);

        numberOfValues = aNumberOfKeys + extraValues;
        updateNumberOfValues(numberOfValues);

        numberOfKeys = aNumberOfKeys;
        updateNumberOfKeys(numberOfKeys);
    }

    void splitValues(final int aNumberOfValues, int bNumberOfValues, final BTreeNodeAbstract bNode)
    {
        //copy values
        final byte[] bValuesBuffer = new byte[bNumberOfValues * VALUE_SIZE];
        buffer.getBytes(getLogKeyIndexOffset(aNumberOfValues + bNumberOfValues - 1), bValuesBuffer);
        bNode.buffer.putBytes(getLogKeyIndexOffset(bNumberOfValues - 1), bValuesBuffer);

        freeSizeLeftBytes += (numberOfValues - aNumberOfValues) * VALUE_SIZE;
        numberOfValues = aNumberOfValues;
        updateNumberOfValues(numberOfValues);
    }

    void setDirty()
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
    private void copyKeyValuesWithGap(final int gapIndex)
    {
        assert gapIndex >= 0;

        final int extraValues = numberOfValues - numberOfKeys;
        final int pairsToMove = numberOfKeys - gapIndex;

        assert extraValues >= 0;
        assert pairsToMove >= 0;

        final long oldIndexOffset = getKeyIndexOffset(gapIndex);
        final long newIndexOffset = getKeyIndexOffset(gapIndex + 1);

        //TODO: this can be optimized with unsafe memory copy
        final int size = (pairsToMove + extraValues) * (KEY_SIZE + VALUE_SIZE);
        byte[] bufferForElementsToMove = new byte[size];

        buffer.getBytes(oldIndexOffset, bufferForElementsToMove);
        buffer.putBytes(newIndexOffset, bufferForElementsToMove);
    }

    private void copyKeyValuesExcept(final int removeIndex)
    {
        assert removeIndex >= 0;

        final int extraValues = numberOfValues - numberOfKeys;
        final int pairsToMove = (numberOfValues + numberOfKeys) - removeIndex;

        assert extraValues >= 0;
        assert pairsToMove >= 0;

        final long oldIndexOffset = getKeyIndexOffset(removeIndex);
        final long newIndexOffset = getKeyIndexOffset(removeIndex + 1);

        //TODO: this can be optimized with unsafe memory copy
        final int size = (pairsToMove * (KEY_SIZE + VALUE_SIZE)) + (extraValues * VALUE_SIZE);
        byte[] bufferForElementsToMove = new byte[size];

        buffer.getBytes(newIndexOffset, bufferForElementsToMove);
        buffer.putBytes(oldIndexOffset, bufferForElementsToMove);
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param gapIndex the index of the gap
     */
    private void copyLogKeyValuesWithGap(final int gapIndex)
    {
        if (gapIndex < numberOfLogKeyValues)
        {
            final int elementsToMove = numberOfLogKeyValues - gapIndex;
            final long oldLogKeyValueIndexOffset = getLogKeyIndexOffset(numberOfLogKeyValues - 1);
            final long newLogKeyValueIndexOffset = getLogKeyIndexOffset(numberOfLogKeyValues);
            byte[] bufferForElementsToMove = new byte[elementsToMove * (KEY_SIZE + VALUE_SIZE)];

            buffer.getBytes(oldLogKeyValueIndexOffset, bufferForElementsToMove);
            buffer.putBytes(newLogKeyValueIndexOffset, bufferForElementsToMove);
        }
    }

    private void copyLogKeyValuesExcept(final int removeIndex)
    {
        if (numberOfLogKeyValues > 0 && removeIndex < (numberOfLogKeyValues - 1))
        {
            final int elementsToMove = numberOfLogKeyValues - removeIndex;
            final long oldLogKeyValueIndexOffset = getLogKeyIndexOffset(numberOfLogKeyValues);
            final long newLogKeyValueIndexOffset = getLogKeyIndexOffset(numberOfLogKeyValues - 1);
            byte[] bufferForElementsToMove = new byte[elementsToMove * (KEY_SIZE + VALUE_SIZE)];

            buffer.getBytes(oldLogKeyValueIndexOffset, bufferForElementsToMove);
            buffer.putBytes(newLogKeyValueIndexOffset, bufferForElementsToMove);
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

    int logBinarySearch(final long key)
    {
        int low = 0;
        int high = numberOfLogKeyValues - 1;
        int index = high >>> 1;

        while (low <= high)
        {
            final long existingKey = getLogKey(index);
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

        if (isRoot())
        {
            contentBuilder.append(
                    String.format(" isRoot : true, previousRoot: %d, timestamp: %d, version: %d,",
                            getPreviousRoot(),
                            getTimestamp(),
                            getVersion()));
        }

        if (numberOfLogKeyValues > 0)
        {
            contentBuilder.append(" log KV : ");
            for (int i = 0; i < numberOfLogKeyValues; i++)
            {
                contentBuilder.append(getLogKey(i));
                contentBuilder.append("-");
                contentBuilder.append(getLogValueFromBuffer(i));
                if (i + 1 != numberOfLogKeyValues)
                {
                    contentBuilder.append(",");
                }
            }
        }

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

        return String.format("%s{ %s }",
                getNodeType().name(),
                contentBuilder.toString());
    }
}
