package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;
import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.nio.charset.Charset;

import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.SHORT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_SIZE;

final class KeyValueLogImpl implements KeyValueLog
{
    static final @ByteOffset int NUMBER_LOG_ENTRIES_OFFSET = ZERO_OFFSET;
    static final @ByteSize int NUMBER_LOG_ENTRIES_SIZE = INT_BYTES_SIZE;

    static final @ByteOffset int TOP_LOG_HEAP_SIZE_OFFSET = StorageUnits.offset(NUMBER_LOG_ENTRIES_OFFSET + NUMBER_LOG_ENTRIES_SIZE);
    static final @ByteSize int TOP_LOG_HEAP_SIZE = SHORT_BYTES_SIZE;

    private static final @ByteOffset int LOG_CELL_OFFSET = StorageUnits.offset(TOP_LOG_HEAP_SIZE_OFFSET + TOP_LOG_HEAP_SIZE);

    private static final @ByteSize int CELL_PAGE_OFFSET_SIZE = SHORT_BYTES_SIZE;
    private static final @ByteSize int CELL_KEY_LENGTH_SIZE = SHORT_BYTES_SIZE;
    private static final @ByteSize int CELL_VALUE_LENGTH_SIZE = SHORT_BYTES_SIZE;
    static final @ByteSize int CELL_SIZE = CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE + CELL_VALUE_LENGTH_SIZE;

    private final Memory keyValuesBuffer;

    private short numberOfLogEntries;

    private KeyValueLogImpl(final Memory keyValuesBuffer, final short numberOfLogEntries)
    {
        assert keyValuesBuffer.getCapacity() <= Short.MAX_VALUE
                : "ke value log cannot be bigger than " + Short.MAX_VALUE + ", provided " + keyValuesBuffer.getCapacity();

        this.keyValuesBuffer = keyValuesBuffer;
        this.numberOfLogEntries = numberOfLogEntries;
    }

    public static KeyValueLogImpl create(final Memory buffer)
    {
        assert buffer.getCapacity() <= Short.MAX_VALUE
                : "ke value log cannot be bigger than " + Short.MAX_VALUE + ", provided " + buffer.getCapacity();

        //TODO: ugh...
        short numberOfLogEntries = 0;
        if (!(buffer instanceof DirectMemory) ||
                (buffer instanceof DirectMemory) && ((DirectMemory) buffer).isInitialized())
        {
            //TODO: more ugh...
            if (buffer.getShort(TOP_LOG_HEAP_SIZE_OFFSET) == 0)
            {
                buffer.putShort(TOP_LOG_HEAP_SIZE_OFFSET, (short) buffer.getCapacity());
            }

            numberOfLogEntries = buffer.getShort(NUMBER_LOG_ENTRIES_OFFSET);
        }

        return new KeyValueLogImpl(buffer, numberOfLogEntries);
    }

    @Override
    public void cacheNumberOfLogPairs()
    {
        this.numberOfLogEntries = keyValuesBuffer.getShort(NUMBER_LOG_ENTRIES_OFFSET);
    }

    @Override
    public void resetLog()
    {
        this.keyValuesBuffer.reset();
        this.numberOfLogEntries = 0;

        keyValuesBuffer.putShort(TOP_LOG_HEAP_SIZE_OFFSET, (short)keyValuesBuffer.getCapacity());
    }

    @Override
    public Memory getMemory()
    {
        return keyValuesBuffer;
    }

    long getKeyAtIndex(final int index)
    {
        return BinaryHelper.bytesToLong(getKeyBytesAtIndex(index));
    }

    @Override
    public byte[] getKeyBytesAtIndex(final int index)
    {
        final @ByteOffset short logEntryOffset = StorageUnits.offset(keyValuesBuffer.getShort(getLogIndexOffset(index)));
        final @ByteSize short logEntryKeySize = StorageUnits.size(keyValuesBuffer.getShort(getLogKeyLengthOffset(index)));

        final byte[] keyBytes = new byte[logEntryKeySize];

        keyValuesBuffer.getBytes(logEntryOffset, logEntryKeySize, keyBytes);

        return keyBytes;
    }

    long getValueAtIndex(final int index)
    {
        return BinaryHelper.bytesToLong(getValueBytesAtIndex(index));
    }

    @Override
    public byte[] getValueBytesAtIndex(final int index)
    {
        final @ByteOffset short logEntryOffset = StorageUnits.offset(keyValuesBuffer.getShort(getLogIndexOffset(index)));
        final @ByteSize short logEntryKeySize = StorageUnits.size(keyValuesBuffer.getShort(getLogKeyLengthOffset(index)));
        final @ByteSize short logEntryValueSize = StorageUnits.size(keyValuesBuffer.getShort(getLogValueLengthOffset(index)));
        final @ByteOffset int valueOffset = StorageUnits.offset(logEntryOffset + logEntryKeySize);

        final byte[] valueBytes = new byte[logEntryValueSize];

        keyValuesBuffer.getBytes(valueOffset, logEntryValueSize, valueBytes);

        return valueBytes;
    }

    byte[] getValue(final byte[] key)
    {
        final int index = binarySearchInLog(key);
        if (index >= 0)
        {
            return getValueBytesAtIndex(index);
        }

        return null;
    }

    @Override
    public void splitLog(final byte[] key, final KeyValueLog bKeyValueLog)
    {
        final int keyIndex = binarySearchInLog(key);
        final int aLogKeyValues = keyIndex + 1;
        final int bLogKeyValues = numberOfLogEntries - aLogKeyValues;

        final int index = aLogKeyValues;
        for (int i = 0; i < bLogKeyValues; i++)
        {
            //TODO: consider one function that returns both objects
            final byte[] keyBytes = getKeyBytesAtIndex(index);
            final byte[] valueBytes = getValueBytesAtIndex(index);

            bKeyValueLog.putKeyValue(i, keyBytes, valueBytes);

            removeLogBytes(keyBytes);
        }
    }

    @Override
    public KeyValueLogImpl spillLog()
    {
        assert keyValuesBuffer.getCapacity() < Integer.MAX_VALUE;

        final HeapMemory spilledKeyValueLogBuffer = MemoryFactory.allocateHeap((int)keyValuesBuffer.getCapacity(), keyValuesBuffer.getByteOrder());
        keyValuesBuffer.getBytes(spilledKeyValueLogBuffer.getCapacity(), spilledKeyValueLogBuffer.getArray());
        resetLog();

        return KeyValueLogImpl.create(spilledKeyValueLogBuffer);
    }

    @Override
    public void insertLog(final byte[] key, final byte[] value)
    {
        final int index = binarySearchInLog(key);
        if (index < 0)
        {
            final int absIndex = -index - 1;
            putKeyValue(absIndex, key, value);
        }
        else
        {
            setLogValue(index, value);
        }
    }

    @Override
    public void putKeyValue(final int index, final byte[] key, final byte[] value)
    {
        assert key.length < Short.MAX_VALUE;
        assert value.length < Short.MAX_VALUE;

        assert key.length + value.length + CELL_SIZE < keyValuesBuffer.getCapacity() - getUsedSize()
                :
                String.format("cannot insert pair due to insufficient capacity. Current Max Capacity : %d Used Capacity: %d, Required: %d",
                        keyValuesBuffer.getCapacity(),
                        getUsedSize(),
                        key.length + value.length + CELL_SIZE);

        copyKeyValueCellsWithGap(index);

        final @ByteOffset short pageRelativeOffset = appendKeyAndValue(key, value);

        keyValuesBuffer.putShort(getLogIndexOffset(index), pageRelativeOffset);
        keyValuesBuffer.putShort(getLogKeyLengthOffset(index), (short)key.length);
        keyValuesBuffer.putShort(getLogValueLengthOffset(index), (short)value.length);

        numberOfLogEntries++;

        setNumberOfPairs(numberOfLogEntries);
    }

    private void setLogValue(final int index, final byte[] value)
    {
        final @ByteSize int newValueLength = StorageUnits.size(value.length);

        assert newValueLength < Short.MAX_VALUE;
        assert (numberOfLogEntries > 0 && index >= 0 && index <= (numberOfLogEntries - 1))
                : String.format("Index %d outside of the range of pairs [0, %d]", index, numberOfLogEntries - 1);

        final @ByteSize short existingValueLength = StorageUnits.size(keyValuesBuffer.getShort(getLogValueLengthOffset(index)));
        final @ByteSize int lengthDifferenceFromExistingPerpective = StorageUnits.size(existingValueLength - newValueLength);

        assert lengthDifferenceFromExistingPerpective < keyValuesBuffer.getCapacity() - getUsedSize()
                : String.format("cannot insert pair due to insufficient capacity. Current Max Capacity : %d Used Capacity: %d, Required: %d",
                        keyValuesBuffer.getCapacity(),
                        getUsedSize(),
                        newValueLength);

        if (lengthDifferenceFromExistingPerpective != 0)
        {
            final @ByteOffset short originalOffset = StorageUnits.offset(keyValuesBuffer.getShort(getLogIndexOffset(index)));
            final @ByteSize short keyLength = StorageUnits.size(keyValuesBuffer.getShort(getLogKeyLengthOffset(index)));
            final @ByteOffset int valueOriginalOffset = StorageUnits.offset(originalOffset + keyLength);

            final @ByteOffset int oldLogKeyValueIndexOffset = getTopHeapOffset();
            final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(getTopHeapOffset() + lengthDifferenceFromExistingPerpective);

            final @ByteSize int sizeToMove = StorageUnits.size(valueOriginalOffset - oldLogKeyValueIndexOffset);

            MemoryCopy.copy(keyValuesBuffer, oldLogKeyValueIndexOffset, keyValuesBuffer, newLogKeyValueIndexOffset, sizeToMove);

            final @ByteSize int lengthDifferenceSize = StorageUnits.size(newValueLength - existingValueLength);
            updateCellOffsetByDifference(index, ZERO_OFFSET, ZERO_SIZE, lengthDifferenceSize);
            updateCellOffsetsByDifferenceFromIndex(originalOffset, lengthDifferenceFromExistingPerpective, ZERO_SIZE, ZERO_SIZE);

            final @ByteOffset int newOffset = StorageUnits.offset(keyValuesBuffer.getShort(getLogIndexOffset(index)));
            final @ByteOffset int newValueOffset = StorageUnits.offset(newOffset + keyLength);
            keyValuesBuffer.putBytes(newValueOffset, value);

            popBytesFromLogHeap(lengthDifferenceFromExistingPerpective);
        }
    }

    private @ByteOffset short appendKeyAndValue(final byte[] key, final byte[] value)
    {
        assert key.length <= Short.MAX_VALUE : "Key size must be below " + Short.MAX_VALUE + ", provided " + key.length;
        assert value.length <= Short.MAX_VALUE : "Value size must be below " + Short.MAX_VALUE + ", provided " + value.length;

        final @ByteSize int keyValueTotalSize = StorageUnits.size(key.length + value.length);
        pushBytesToLogHeap(keyValueTotalSize);

        final @ByteOffset short topLogHeapOffset = getTopHeapOffset();

        keyValuesBuffer.putBytes(topLogHeapOffset, key);
        keyValuesBuffer.putBytes(StorageUnits.offset(topLogHeapOffset + key.length), value);

        return topLogHeapOffset;
    }

    private void popBytesFromLogHeap(final @ByteSize int length)
    {
        @ByteOffset short topKeyValueHeapOffset = getTopHeapOffset();

        topKeyValueHeapOffset += StorageUnits.offset(length);

        keyValuesBuffer.putShort(TOP_LOG_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    private void pushBytesToLogHeap(final @ByteSize int length)
    {
        @ByteOffset short topKeyValueHeapOffset = getTopHeapOffset();

        topKeyValueHeapOffset -= StorageUnits.offset(length);

        keyValuesBuffer.putShort(TOP_LOG_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    private @ByteOffset short getTopHeapOffset()
    {
        return StorageUnits.offset(keyValuesBuffer.getShort(TOP_LOG_HEAP_SIZE_OFFSET));
    }

    private static @ByteOffset long getLogKeyLengthOffset(final int index)
    {
        return StorageUnits.offset(getLogIndexOffset(index) + CELL_PAGE_OFFSET_SIZE);
    }

    private static @ByteOffset long getLogValueLengthOffset(final int index)
    {
        return StorageUnits.offset(getLogIndexOffset(index) + CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE);
    }

    private static @ByteOffset long getLogIndexOffset(final int index)
    {
        return StorageUnits.offset(LOG_CELL_OFFSET + (index * CELL_SIZE));
    }

    private void setNumberOfPairs(final int numberOfLogKeyValues)
    {
        keyValuesBuffer.putInt(NUMBER_LOG_ENTRIES_OFFSET, numberOfLogKeyValues);
    }

    @Override
    public int getNumberOfPairs()
    {
        return numberOfLogEntries;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param gapIndex the index of the gap
     */
    private void copyKeyValueCellsWithGap(final int gapIndex)
    {
        assert gapIndex >= 0;

        final @ByteOffset long oldIndexOffset = getLogIndexOffset(gapIndex);
        final @ByteOffset long newIndexOffset = getLogIndexOffset(gapIndex + 1);

        final int numberOfEntriesToMove = numberOfLogEntries - gapIndex;
        final @ByteSize int size = StorageUnits.size(numberOfEntriesToMove * CELL_SIZE);
        if (size > 0)
        {
            MemoryCopy.copy(keyValuesBuffer, oldIndexOffset, keyValuesBuffer, newIndexOffset, size);
        }
    }

    @Override
    public boolean removeLogBytes(final byte[] key)
    {
        final int index = binarySearchInLog(key);
        if (index >= 0)
        {
            removeLogAtIndex(index);
            return true;
        }

        return false;
    }

    private void removeLogAtIndex(final int index)
    {
        copyKeyValuesExcept(index);
        copyKeyValueCellsExcept(index);

        numberOfLogEntries--;
        setNumberOfPairs(numberOfLogEntries);
    }

    @Override
    public int binarySearchInLog(final byte[] key)
    {
        return SearchUtils.binarySearch(key, numberOfLogEntries, this::getKeyBytesAtIndex, ByteArrayComparator.INSTANCE);
    }

    private void copyKeyValuesExcept(final int removeIndex)
    {
        assert  (numberOfLogEntries > 0 && removeIndex <= (numberOfLogEntries - 1))
                : String.format("invalid index to remove %d from range [0, %d]", removeIndex, numberOfLogEntries - 1);

        final @ByteOffset short offset = StorageUnits.offset(keyValuesBuffer.getShort(getLogIndexOffset(removeIndex)));
        final @ByteSize int keyLength = StorageUnits.size(keyValuesBuffer.getShort(getLogKeyLengthOffset(removeIndex)));
        final @ByteSize int valueLength = StorageUnits.size(keyValuesBuffer.getShort(getLogValueLengthOffset(removeIndex)));
        final @ByteSize int totalRemoveLength = StorageUnits.size(keyLength + valueLength);

        final @ByteOffset int oldLogKeyValueIndexOffset = getTopHeapOffset();
        final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(getTopHeapOffset() + totalRemoveLength);

        final @ByteSize int sizeToMove = StorageUnits.size(offset - oldLogKeyValueIndexOffset);

        MemoryCopy.copy(keyValuesBuffer, oldLogKeyValueIndexOffset, keyValuesBuffer, newLogKeyValueIndexOffset, sizeToMove);

        updateCellOffsetsByDifferenceFromIndex(offset, totalRemoveLength, ZERO_SIZE, ZERO_SIZE);

        popBytesFromLogHeap(totalRemoveLength);
    }

    private void updateCellOffsetsByDifferenceFromIndex(
            final @ByteOffset short offsetMoved,
            final @ByteSize int totalMovedLength,
            final @ByteSize int keyLengthDifference,
            final @ByteSize int valueLengthDifference)
    {
        for (int i = 0; i < numberOfLogEntries; i++)
        {
            final @ByteSize int offset = StorageUnits.size(keyValuesBuffer.getShort(getLogIndexOffset(i)));

            if (offset <= offsetMoved)
            {
                updateCellOffsetByDifference(i, totalMovedLength, keyLengthDifference, valueLengthDifference);
            }
        }
    }

    private void updateCellOffsetByDifference(
            final int index,
            final @ByteSize int offsetDifference,
            final @ByteSize int keyLengthDifference,
            final @ByteSize int valueLengthDifference)
    {
        if (offsetDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellIndexOffset = getLogIndexOffset(index);
            final @ByteOffset short oldPairOffset = StorageUnits.offset(keyValuesBuffer.getShort(cellIndexOffset));
            final @ByteOffset short newPairOffset = StorageUnits.offset((short) (oldPairOffset + offsetDifference));
            keyValuesBuffer.putShort(cellIndexOffset, newPairOffset);
        }

        if (keyLengthDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellKeyLengthOffset = getLogKeyLengthOffset(index);
            final @ByteSize short oldKeyLength = StorageUnits.size(keyValuesBuffer.getShort(cellKeyLengthOffset));
            final @ByteSize short newKeyLength = StorageUnits.size((short) (oldKeyLength + keyLengthDifference));
            keyValuesBuffer.putShort(cellKeyLengthOffset, newKeyLength);
        }

        if (valueLengthDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellValueLengthOffset = getLogValueLengthOffset(index);
            final @ByteSize short oldValueLength = StorageUnits.size(keyValuesBuffer.getShort(cellValueLengthOffset));
            final @ByteSize short newValueLength = StorageUnits.offset((short) (oldValueLength + valueLengthDifference));
            keyValuesBuffer.putShort(cellValueLengthOffset, newValueLength);
        }
    }

    private void copyKeyValueCellsExcept(final int removeIndex)
    {
        assert removeIndex >= 0;

        final int pairsToMove = numberOfLogEntries - removeIndex;

        assert pairsToMove >= 0;

        final @ByteOffset long oldIndexOffset = getLogIndexOffset(removeIndex);
        final @ByteOffset long newIndexOffset = getLogIndexOffset(removeIndex + 1);

        final @ByteSize int size = StorageUnits.size(pairsToMove * CELL_SIZE);
        MemoryCopy.copy(keyValuesBuffer, newIndexOffset, keyValuesBuffer, oldIndexOffset, size);
    }

    @Override
    public @ByteSize long getUsedSize()
    {
        final @ByteSize long logHeapSize = StorageUnits.size(keyValuesBuffer.getCapacity() - getTopHeapOffset());
        final @ByteSize long logCellArraySize = StorageUnits.size(numberOfLogEntries * CELL_SIZE);
        return KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE + logHeapSize + logCellArraySize;
    }

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder();

        if (numberOfLogEntries > 0)
        {
            Charset utf8 = Charset.forName("UTF-8");
            contentBuilder.append("KeyValueLog : ");
            for (int i = 0; i < numberOfLogEntries; i++)
            {
                contentBuilder.append(new String(getKeyBytesAtIndex(i), utf8));
                contentBuilder.append("-");
                contentBuilder.append(new String(getValueBytesAtIndex(i), utf8));
                if (i + 1 != numberOfLogEntries)
                {
                    contentBuilder.append(",");
                }
            }
        }

        return contentBuilder.toString();
    }

    String printDebug()
    {
        final StringBuilder contentBuilder = new StringBuilder();
        if (numberOfLogEntries > 0)
        {
            Charset utf8 = Charset.forName("UTF-8");
            contentBuilder.append("KeyValueLog : ");
            for (int i = 0; i < numberOfLogEntries; i++)
            {
                final byte[] keyBytes = getKeyBytesAtIndex(i);
                final byte[] valueBytes = getValueBytesAtIndex(i);

                final String keyLong = keyBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(keyBytes))
                        : "";

                final String valueLong = valueBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(valueBytes))
                        : "";

                contentBuilder.append(System.lineSeparator())
                        .append("Cell ").append(i)
                        .append(", offset ").append(keyValuesBuffer.getShort(getLogIndexOffset(i)))
                        .append(", keyLength ").append(keyValuesBuffer.getShort(getLogKeyLengthOffset(i)))
                        .append(", valueLength ").append(keyValuesBuffer.getShort(getLogValueLengthOffset(i)))
                        .append(", key ").append(new String(keyBytes, utf8)).append(keyLong)
                        .append(", value ").append(new String(valueBytes, utf8)).append(valueLong);
            }
        }

        return contentBuilder.toString();
    }
}
