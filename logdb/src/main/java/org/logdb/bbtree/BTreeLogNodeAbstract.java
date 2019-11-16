package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public abstract class BTreeLogNodeAbstract extends BTreeNodeAbstract implements BTreeLogNode
{
    int numberOfLogKeyValues;

    private final KeyValueLog keyValueLog;

    BTreeLogNodeAbstract(
            final @PageNumber long pageNumber,
            final Memory memory,
            final int numberOfLogKeyValues,
            final int numberOfKeys,
            final int numberOfValues)
    {
        super(pageNumber, memory, numberOfKeys, numberOfValues);
        this.numberOfLogKeyValues = numberOfLogKeyValues;
        this.keyValueLog = new KeyValueLog(memory);
    }

    @Override
    public void reset()
    {
        super.reset();
        numberOfLogKeyValues = 0;
    }

    @Override
    public int getLogKeyValuesCount()
    {
        return numberOfLogKeyValues;
    }

    @Override
    public boolean shouldSplit()
    {
        final long freeSpaceWithoutConsideringLogBuffer =
                freeSizeLeftBytes + (numberOfLogKeyValues * (long)(BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
        final int minimumFreeSpaceBeforeOperatingOnNode = 2 * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        return minimumFreeSpaceBeforeOperatingOnNode > freeSpaceWithoutConsideringLogBuffer;
    }

    void splitLog(final long key, final BTreeLogNodeAbstract bNode)
    {
        final int keyIndex = binarySearchInLog(key);
        final int aLogKeyValues = keyIndex + 1;
        final int bLogKeyValues = numberOfLogKeyValues - aLogKeyValues;
        final @ByteOffset long sourceOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues);
        final @ByteOffset long destinationOffset = bNode.keyValueLog.getLogKeyIndexOffset(bLogKeyValues);
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

    @Override
    long calculateFreeSpaceLeft(final long pageSize)
    {
        final int extraValues = numberOfValues - numberOfKeys;
        final int numberOfKeyValues = numberOfKeys + extraValues + numberOfLogKeyValues;
        final int sizeForKeyValues = numberOfKeyValues * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        final int usedBytes = sizeForKeyValues + BTreeNodePage.HEADER_SIZE_BYTES;
        return pageSize - usedBytes;
    }

    KeyValueLog spillLog()
    {
        final @ByteSize int keyValueLogSize = StorageUnits.size(numberOfLogKeyValues * 2 * Long.BYTES);
        final HeapMemory keyValueLogBuffer = MemoryFactory.allocateHeap(keyValueLogSize, buffer.getByteOrder());

        final @ByteOffset long logStartOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues - 1);
        buffer.getBytes(
                logStartOffset,
                keyValueLogBuffer.getCapacity(),
                keyValueLogBuffer.getArray());

        final KeyValueLog keyValueLog = new KeyValueLog(keyValueLogBuffer);

        assert keyValueLog.getNumberOfElements() % 2 == 0
                : "log key/value array must even size. Current size " + keyValueLog.getNumberOfElements();

        updateNumberOfLogKeyValues(0);
        recalculateFreeSpaceLeft();

        return keyValueLog;
    }

    @Override
    public boolean hasKeyLog(long key)
    {
        final int logIndex = binarySearchInLog(key);
        return logIndex >= 0;
    }

    @Override
    public long getLogValue(final long key)
    {
        final int logIndex = binarySearchInLog(key);
        if (logIndex >= 0)
        {
            return getLogValueAtIndex(logIndex);
        }
        else
        {
            return KEY_NOT_FOUND_VALUE;
        }
    }

    @Override
    public long getLogValueAtIndex(final int index)
    {
        return keyValueLog.getValue(index);
    }

    long getLogKey(final int index)
    {
        return keyValueLog.getKey(index);
    }

    int binarySearchInLog(final long key)
    {
        return SearchUtils.binarySearch(key, numberOfLogKeyValues, this::getLogKey);
    }

    private void updateNumberOfLogKeyValues(final int numberOfLogKeyValues)
    {
        this.numberOfLogKeyValues = numberOfLogKeyValues;
        buffer.putInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET, numberOfLogKeyValues);
    }

    void insertLog(final long key, final long value)
    {
        final int index = binarySearchInLog(key);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            insertLogKeyValue(absIndex, key, value);
        }
        else
        {
            setLogKeyValue(index, key, value);
        }

        setDirty();
    }

    private void setLogKeyValue(final int index, final long key, final long value)
    {
        keyValueLog.putKeyValue(index, key, value);
    }

    private void insertLogKeyValue(final int index, final long key, final long value)
    {
        copyLogKeyValuesWithGap(index);
        setLogKeyValue(index, key, value);

        numberOfLogKeyValues++;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        recalculateFreeSpaceLeft();
    }

    @Override
    public void removeLog(final long key)
    {
        final int index = binarySearchInLog(key);
        if (index >= 0)
        {
            removeLogAtIndex(index);
            setDirty();
        }
    }

    @Override
    public void removeLogAtIndex(final int index)
    {
        copyLogKeyValuesExcept(index);

        numberOfLogKeyValues--;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        recalculateFreeSpaceLeft();
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
            final @ByteOffset long oldLogKeyValueIndexOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues - 1);
            final @ByteOffset long newLogKeyValueIndexOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues);
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
            final @ByteOffset long oldLogKeyValueIndexOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues);
            final @ByteOffset long newLogKeyValueIndexOffset = keyValueLog.getLogKeyIndexOffset(numberOfLogKeyValues - 1);
            final @ByteSize int size =
                    StorageUnits.size(elementsToMove * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
            MemoryCopy.copy(buffer, oldLogKeyValueIndexOffset, buffer, newLogKeyValueIndexOffset, size);
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder(super.toString());

        if (numberOfLogKeyValues > 0)
        {
            contentBuilder.append(" log KV : ");
            for (int i = 0; i < numberOfLogKeyValues; i++)
            {
                contentBuilder.append(getLogKey(i));
                contentBuilder.append("-");
                contentBuilder.append(getLogValueAtIndex(i));
                if (i + 1 != numberOfLogKeyValues)
                {
                    contentBuilder.append(",");
                }
            }
        }

        return contentBuilder.toString();
    }
}
