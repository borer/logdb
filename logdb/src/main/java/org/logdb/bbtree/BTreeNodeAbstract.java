package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

abstract class BTreeNodeAbstract implements BTreeNode
{
    private long freeSizeLeftBytes;

    @PageNumber long pageNumber;
    int numberOfKeys;
    int numberOfValues;
    int numberOfLogKeyValues;
    boolean isDirty;

    final Memory buffer;

    /**
     * Constructor.
     *
     * @param pageNumber     the page number of this node or an id generated for not yet persisted nodes
     * @param buffer         the buffer used as a content for this node
     * @param numberOfKeys   number of keys in this node
     * @param numberOfValues number of values in this node
     */
    BTreeNodeAbstract(final @PageNumber long pageNumber,
                      final Memory buffer,
                      final int numberOfLogKeyValues,
                      final int numberOfKeys,
                      final int numberOfValues)
    {
        this.pageNumber = pageNumber;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.numberOfKeys = numberOfKeys;
        this.numberOfValues = numberOfValues;
        this.numberOfLogKeyValues = numberOfLogKeyValues;
        this.freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
        this.isDirty = true;
    }

    void preCommit(
            final boolean isRoot,
            final @PageNumber long previousRootPageNumber,
            final @Milliseconds long timestamp,
            final @Version long version)
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

    private void preCommitRoot(
            final @PageNumber long previousRootPageNumber,
            final @Milliseconds long timestamp,
            final @Version long version)
    {
        setNodePageType(getNodeType());
        setRootFlag(true);
        setPreviousRoot(previousRootPageNumber);
        setTimestamp(timestamp);
        setVersion(version);
        isDirty = false;
    }

    @Override
    public void reset()
    {
        freeSizeLeftBytes = 0;
        pageNumber = StorageUnits.INVALID_PAGE_NUMBER;
        numberOfKeys = 0;
        numberOfValues = 0;
        numberOfLogKeyValues = 0;
        isDirty = false;

        final ByteBuffer supportByteBufferIfAny = buffer.getSupportByteBufferIfAny();
        if (supportByteBufferIfAny != null)
        {
            Arrays.fill(supportByteBufferIfAny.array(), (byte)0);
        }
    }

    private void setNodePageType(final BtreeNodeType type)
    {
        buffer.putByte(BTreeNodePage.PAGE_TYPE_OFFSET, type.getType());
    }

    void setRootFlag(final boolean isRoot)
    {
        buffer.putByte(BTreeNodePage.PAGE_IS_ROOT_OFFSET, (byte) (isRoot ? 1 : 0));
    }

    public boolean isRoot()
    {
        return buffer.getByte(BTreeNodePage.PAGE_IS_ROOT_OFFSET) == 1;
    }

    void setTimestamp(final @Milliseconds long timestamp)
    {
        buffer.putLong(BTreeNodePage.PAGE_TIMESTAMP_OFFSET, timestamp);
    }

    public @Milliseconds long getTimestamp()
    {
        return TimeUnits.millis(buffer.getLong(BTreeNodePage.PAGE_TIMESTAMP_OFFSET));
    }

    @Override
    public void setVersion(final @Version long version)
    {
        buffer.putLong(BTreeNodePage.PAGE_VERSION_OFFSET, version);
    }

    @Override
    public @Version long getVersion()
    {
        return StorageUnits.version(buffer.getLong(BTreeNodePage.PAGE_VERSION_OFFSET));
    }

    @Override
    public boolean shouldSplit()
    {
        final long freeSpaceWithoutConsideringLogBuffer =
                freeSizeLeftBytes + (numberOfLogKeyValues * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
        final int minimumFreeSpaceBeforeOperatingOnNode = 2 * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        return minimumFreeSpaceBeforeOperatingOnNode > freeSpaceWithoutConsideringLogBuffer;
    }

    void setPreviousRoot(final @PageNumber long previousRootPageNumber)
    {
        buffer.putLong(BTreeNodePage.PAGE_PREV_OFFSET, previousRootPageNumber);
    }

    public @PageNumber long getPreviousRoot()
    {
        return StorageUnits.pageNumber(buffer.getLong(BTreeNodePage.PAGE_PREV_OFFSET));
    }

    @Override
    public @PageNumber long getPageNumber()
    {
        return pageNumber;
    }

    public void initNodeFromBuffer()
    {
        numberOfKeys = buffer.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET);
        numberOfValues = buffer.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET);

        if (getNodeType() == BtreeNodeType.NonLeaf)
        {
            numberOfLogKeyValues = buffer.getInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET);
        }
        else
        {
            numberOfLogKeyValues = 0;
        }

        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    private long calculateFreeSpaceLeft(final long pageSize)
    {
        final int extraValues = numberOfValues - numberOfKeys;
        final int numberOfKeyValues = numberOfKeys + extraValues + numberOfLogKeyValues;
        final int sizeForKeyValues = numberOfKeyValues * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        final int usedBytes = sizeForKeyValues + BTreeNodePage.HEADER_SIZE_BYTES;
        return pageSize - usedBytes;
    }

    private void recalculateFreeSpaceLeft()
    {
        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    @Override
    public int getKeyCount()
    {
        return numberOfKeys;
    }

    int getLogKeyValuesCount()
    {
        return numberOfLogKeyValues;
    }

    @Override
    public long getKey(final int index)
    {
        return buffer.getLong(getKeyIndexOffset(index));
    }

    @Override
    public long getMinKey()
    {
        return buffer.getLong(getKeyIndexOffset(0));
    }

    @Override
    public long getMaxKey()
    {
        return buffer.getLong(getKeyIndexOffset(numberOfKeys - 1));
    }

    private static @ByteOffset long getKeyIndexOffset(final int index)
    {
        return StorageUnits.offset(BTreeNodePage.KEY_START_OFFSET + (index * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE)));
    }

    private static @ByteOffset long getValueIndexOffsetNew(final int index)
    {
        return StorageUnits.offset(getKeyIndexOffset(index) + BTreeNodePage.KEY_SIZE);
    }

    long getLogKey(final int index)
    {
        return buffer.getLong(getLogKeyIndexOffset(buffer.getCapacity(), index));
    }

    long getLogValue(final int index)
    {
        return buffer.getLong(getLogValueIndexOffset(buffer.getCapacity(), index));
    }

    private static @ByteOffset long getLogKeyIndexOffset(final long pageSize, final int index)
    {
        return StorageUnits.offset(pageSize - ((index + 1) * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE)));
    }

    private static @ByteOffset long getLogValueIndexOffset(final long pageSize, final int index)
    {
        return StorageUnits.offset(getLogKeyIndexOffset(pageSize, index) + BTreeNodePage.KEY_SIZE);
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

        recalculateFreeSpaceLeft();
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

        recalculateFreeSpaceLeft();
    }

    void updateNumberOfKeys(final int numberOfKeys)
    {
        this.numberOfKeys = numberOfKeys;
        buffer.putInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET, numberOfKeys);
    }

    void updateNumberOfValues(final int numberOfValues)
    {
        this.numberOfValues = numberOfValues;
        buffer.putInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET, numberOfValues);
    }

    void updateNumberOfLogKeyValues(final int numberOfLogKeyValues)
    {
        this.numberOfLogKeyValues = numberOfLogKeyValues;
        buffer.putInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET, numberOfLogKeyValues);
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

    public boolean logHasFreeSpace()
    {
        return freeSizeLeftBytes > (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
    }

    void removeLogKeyValue(final int index)
    {
        copyLogKeyValuesExcept(index);

        numberOfLogKeyValues--;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        recalculateFreeSpaceLeft();
    }

    void insertLogKeyValue(final int index, final long key, final long value)
    {
        copyLogKeyValuesWithGap(index);
        setLogKeyValue(index, key, value);

        numberOfLogKeyValues++;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        recalculateFreeSpaceLeft();
    }

    void setLogKeyValue(final int index, final long key, final long value)
    {
        buffer.putLong(getLogKeyIndexOffset(buffer.getCapacity(), index), key); //TODO: maybe we don't need to set the key again.
        buffer.putLong(getLogValueIndexOffset(buffer.getCapacity(), index), value);
    }

    public long[] spillLog()
    {
        //TODO: optimize the copy in bulk
        final long[] keyValueLog = new long[numberOfLogKeyValues * 2];
        for (int i = 0; i < numberOfLogKeyValues; i++)
        {
            final int index = i * 2;
            keyValueLog[index] = getLogKey(i);
            keyValueLog[index + 1] = getLogValue(i);
        }

        updateNumberOfLogKeyValues(0);
        recalculateFreeSpaceLeft();

        return keyValueLog;
    }

    long setValue(final int index, final long value)
    {
        final @ByteOffset long valueIndexOffset = getValueIndexOffsetNew(index);
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
        final @ByteSize int lengthOfSplit =
                StorageUnits.size((bNumberOfKeys + extraValues) * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
        MemoryCopy.copy(buffer, getKeyIndexOffset(numberOfKeys - bNumberOfKeys), bNode.buffer, getKeyIndexOffset(0), lengthOfSplit);

        numberOfValues = aNumberOfKeys + extraValues;
        updateNumberOfValues(numberOfValues);

        numberOfKeys = aNumberOfKeys;
        updateNumberOfKeys(numberOfKeys);

        recalculateFreeSpaceLeft();
        bNode.recalculateFreeSpaceLeft();
    }

    void splitLog(final long key, final BTreeNodeAbstract bNode)
    {
        final int keyIndex = binarySearchInLog(key);
        final int aLogKeyValues = keyIndex + 1;
        final int bLogKeyValues = numberOfLogKeyValues - aLogKeyValues;
        final @ByteOffset long sourceOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues);
        final @ByteOffset long destinationOffset = getLogKeyIndexOffset(bNode.buffer.getCapacity(), bLogKeyValues);
        final @ByteSize int length =
                StorageUnits.size((bLogKeyValues + 1) * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));

        MemoryCopy.copy(buffer,
                sourceOffset,
                bNode.buffer,
                destinationOffset,
                length);

        updateNumberOfLogKeyValues(aLogKeyValues);
        bNode.updateNumberOfLogKeyValues(bLogKeyValues);

        recalculateFreeSpaceLeft();
        bNode.recalculateFreeSpaceLeft();
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

        final @ByteOffset long oldIndexOffset = getKeyIndexOffset(gapIndex);
        final @ByteOffset long newIndexOffset = getKeyIndexOffset(gapIndex + 1);

        final @ByteSize int size =
                StorageUnits.size((pairsToMove + extraValues) * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
        MemoryCopy.copy(buffer, oldIndexOffset, buffer, newIndexOffset, size);
    }

    private void copyKeyValuesExcept(final int removeIndex)
    {
        assert removeIndex >= 0;

        final int numberOfKeysToMove = numberOfKeys - 1;
        final int numberOfValuesToMove = numberOfValues - 1;

        assert numberOfKeysToMove >= 0;
        assert numberOfValuesToMove >= 0;

        final @ByteOffset long oldIndexOffset = getKeyIndexOffset(removeIndex);
        final @ByteOffset long newIndexOffset = getKeyIndexOffset(removeIndex + 1);

        final @ByteSize int size =
                StorageUnits.size((numberOfKeysToMove * BTreeNodePage.KEY_SIZE) + (numberOfValuesToMove * BTreeNodePage.VALUE_SIZE));
        MemoryCopy.copy(buffer, newIndexOffset, buffer, oldIndexOffset, size);
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
            final @ByteOffset long oldLogKeyValueIndexOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues - 1);
            final @ByteOffset long newLogKeyValueIndexOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues);
            final @ByteSize int size =
                    StorageUnits.size(elementsToMove * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
            MemoryCopy.copy(buffer, oldLogKeyValueIndexOffset, buffer, newLogKeyValueIndexOffset, size);
        }
    }

    private void copyLogKeyValuesExcept(final int removeIndex)
    {
        if (numberOfLogKeyValues > 0 && removeIndex < (numberOfLogKeyValues - 1))
        {
            final int elementsToMove = numberOfLogKeyValues - removeIndex;
            final @ByteOffset long oldLogKeyValueIndexOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues);
            final @ByteOffset long newLogKeyValueIndexOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues - 1);
            final @ByteSize int size =
                    StorageUnits.size(elementsToMove * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
            MemoryCopy.copy(buffer, oldLogKeyValueIndexOffset, buffer, newLogKeyValueIndexOffset, size);
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

    int binarySearchInLog(final long key)
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
                contentBuilder.append(getLogValue(i));
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
