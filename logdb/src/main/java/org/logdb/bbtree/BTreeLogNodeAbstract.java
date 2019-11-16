package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

import static org.logdb.bbtree.KeyValueLog.getLogKeyIndexOffset;
import static org.logdb.bbtree.KeyValueLog.getLogValueIndexOffset;

public abstract class BTreeLogNodeAbstract extends BTreeNodeAbstract implements BTreeLogNode
{
    int numberOfLogKeyValues;

    public BTreeLogNodeAbstract(
            final @PageNumber long pageNumber,
            final Memory memory,
            final int numberOfLogKeyValues,
            final int numberOfKeys,
            final int numberOfValues)
    {
        super(pageNumber, memory, numberOfKeys, numberOfValues);
        this.numberOfLogKeyValues = numberOfLogKeyValues;
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
    public long getLogValue(final int index)
    {
        return buffer.getLong(getLogValueIndexOffset(buffer.getCapacity(), index));
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

    @Override
    long calculateFreeSpaceLeft(final long pageSize)
    {
        final int extraValues = numberOfValues - numberOfKeys;
        final int numberOfKeyValues = numberOfKeys + extraValues + numberOfLogKeyValues;
        final int sizeForKeyValues = numberOfKeyValues * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        final int usedBytes = sizeForKeyValues + BTreeNodePage.HEADER_SIZE_BYTES;
        return pageSize - usedBytes;
    }

    HeapMemory spillLog()
    {
        final @ByteSize int keyValueLogSize = StorageUnits.size(numberOfLogKeyValues * 2 * Long.BYTES);
        final HeapMemory keyValueLog = MemoryFactory.allocateHeap(keyValueLogSize, buffer.getByteOrder());

        final @ByteOffset long logStartOffset = getLogKeyIndexOffset(buffer.getCapacity(), numberOfLogKeyValues - 1);
        buffer.getBytes(
                logStartOffset,
                keyValueLog.getCapacity(),
                keyValueLog.getSupportByteBufferIfAny().array());

        assert KeyValueLog.getNumberOfElements(keyValueLog.getCapacity()) % 2 == 0
                : "log key/value array must even size. Current size " + KeyValueLog.getNumberOfElements(keyValueLog.getCapacity());

        updateNumberOfLogKeyValues(0);
        recalculateFreeSpaceLeft();

        return keyValueLog;
    }

    @Override
    public int binarySearchInLog(final long key)
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
        buffer.putLong(getLogKeyIndexOffset(buffer.getCapacity(), index), key);
        buffer.putLong(getLogValueIndexOffset(buffer.getCapacity(), index), value);
    }

    private void insertLogKeyValue(final int index, final long key, final long value)
    {
        copyLogKeyValuesWithGap(index);
        setLogKeyValue(index, key, value);

        numberOfLogKeyValues++;
        updateNumberOfLogKeyValues(numberOfLogKeyValues);

        recalculateFreeSpaceLeft();
    }

    /**
     * try to remove a key/value pair for this node log.
     * @param key the key that identifies the key/value pair to remove from the node log
     * @return true if removed successfully, false if key/value are not in the log.
     */
    boolean removeLogWithKey(final long key)
    {
        final int index = binarySearchInLog(key);
        if (index >= 0)
        {
            removeLogAtIndex(index);
            setDirty();

            return true;
        }

        return false;
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
                contentBuilder.append(getLogValue(i));
                if (i + 1 != numberOfLogKeyValues)
                {
                    contentBuilder.append(",");
                }
            }
        }

        return contentBuilder.toString();
    }
}
