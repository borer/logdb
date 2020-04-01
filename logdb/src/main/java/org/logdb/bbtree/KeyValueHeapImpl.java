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

import java.nio.charset.StandardCharsets;

import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.SHORT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_SIZE_SHORT;

final class KeyValueHeapImpl implements KeyValueHeap
{
    private static final @ByteOffset int NUMBER_ENTRIES_OFFSET = ZERO_OFFSET;
    private static final @ByteSize int NUMBER_ENTRIES_SIZE = SHORT_BYTES_SIZE;

    private static final @ByteOffset int TOP_HEAP_SIZE_OFFSET = StorageUnits.offset(NUMBER_ENTRIES_OFFSET + NUMBER_ENTRIES_SIZE);
    private static final @ByteSize int TOP_HEAP_SIZE = SHORT_BYTES_SIZE;

    static final @ByteSize int HEADER_SIZE = NUMBER_ENTRIES_SIZE + TOP_HEAP_SIZE;

    private static final @ByteOffset int CELL_OFFSET = StorageUnits.offset(HEADER_SIZE);
    private static final @ByteSize int CELL_PAGE_OFFSET_SIZE = SHORT_BYTES_SIZE;
    private static final @ByteSize int CELL_KEY_LENGTH_SIZE = SHORT_BYTES_SIZE;
    private static final @ByteSize int CELL_VALUE_LENGTH_SIZE = SHORT_BYTES_SIZE;

    static final @ByteSize int CELL_SIZE = CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE + CELL_VALUE_LENGTH_SIZE;

    private final Memory keyValuesBuffer;

    private short numberOfEntries;

    private KeyValueHeapImpl(final Memory keyValuesBuffer, final short numberOfEntries)
    {
        assert keyValuesBuffer.getCapacity() <= Short.MAX_VALUE
                : "key value log cannot be bigger than " + Short.MAX_VALUE + ", provided " + keyValuesBuffer.getCapacity();

        this.keyValuesBuffer = keyValuesBuffer;
        this.numberOfEntries = numberOfEntries;
    }

    public static KeyValueHeapImpl create(final Memory buffer)
    {
        return create(buffer, (short) 0);
    }

    public static KeyValueHeapImpl create(final Memory buffer, final short initialNumberOfPairs)
    {
        assert buffer.getCapacity() <= Short.MAX_VALUE
                : "key value log cannot be bigger than " + Short.MAX_VALUE + ", provided " + buffer.getCapacity();

        //TODO: ugh...
        short numberOfLogEntries = initialNumberOfPairs > 0 ? initialNumberOfPairs : 0;
        if (!(buffer instanceof DirectMemory) ||
                (buffer instanceof DirectMemory) && ((DirectMemory) buffer).isInitialized())
        {
            //TODO: more ugh...
            if (buffer.getShort(TOP_HEAP_SIZE_OFFSET) == 0)
            {
                buffer.putShort(TOP_HEAP_SIZE_OFFSET, (short) buffer.getCapacity());
            }

            numberOfLogEntries = numberOfLogEntries == 0 ? buffer.getShort(NUMBER_ENTRIES_OFFSET) : numberOfLogEntries;
        }

        return new KeyValueHeapImpl(buffer, numberOfLogEntries);
    }

    @Override
    public void cacheNumberOfLogPairs()
    {
        this.numberOfEntries = keyValuesBuffer.getShort(NUMBER_ENTRIES_OFFSET);
    }

    @Override
    public void reset(final short numberOfEntries)
    {
        this.keyValuesBuffer.reset();
        this.numberOfEntries = numberOfEntries;

        keyValuesBuffer.putShort(TOP_HEAP_SIZE_OFFSET, (short)keyValuesBuffer.getCapacity());
    }

    @Override
    public Memory getMemory()
    {
        return keyValuesBuffer;
    }

    @Override
    public byte[] getKeyAtIndex(final int index)
    {
        final @ByteOffset short entryOffset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
        final @ByteSize short entryKeySize = StorageUnits.size(keyValuesBuffer.getShort(getKeyLengthOffset(index)));

        final byte[] keyBytes = new byte[entryKeySize];

        keyValuesBuffer.getBytes(entryOffset, entryKeySize, keyBytes);

        return keyBytes;
    }

    @Override
    public byte[] getValueAtIndex(final int index)
    {
        final @ByteOffset short entryOffset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
        final @ByteSize short entryKeySize = StorageUnits.size(keyValuesBuffer.getShort(getKeyLengthOffset(index)));
        final @ByteSize short entryValueSize = StorageUnits.size(keyValuesBuffer.getShort(getValueLengthOffset(index)));
        final @ByteOffset int valueOffset = StorageUnits.offset(entryOffset + entryKeySize);

        final byte[] valueBytes = new byte[entryValueSize];

        if (entryValueSize > 0)
        {
            keyValuesBuffer.getBytes(valueOffset, entryValueSize, valueBytes);
        }

        return valueBytes;
    }

    byte[] getValue(final byte[] key)
    {
        final int index = binarySearch(key);
        if (index >= 0)
        {
            return getValueAtIndex(index);
        }

        return null;
    }

    @Override
    public void split(final byte[] key, final KeyValueHeap newKeyValueHeap)
    {
        final int keyIndex = binarySearch(key);
        final int aKeyValues = keyIndex + 1;
        final int bKeyValues = numberOfEntries - aKeyValues;

        final int index = aKeyValues;
        split(index, bKeyValues, newKeyValueHeap);
    }

    @Override
    public void split(final int index, final int newKeyValues, final KeyValueHeap newKeyValueHeap)
    {
        for (int i = 0; i < newKeyValues; i++)
        {
            //TODO: consider one function that returns both objects
            final byte[] keyBytes = getKeyAtIndex(index);
            final byte[] valueBytes = getValueAtIndex(index);

            newKeyValueHeap.insertAtIndex(i, keyBytes, valueBytes);

            removeKeyValue(keyBytes);
        }
    }

    @Override
    public KeyValueHeapImpl spill()
    {
        assert keyValuesBuffer.getCapacity() < Integer.MAX_VALUE;

        final HeapMemory spilledKeyValueBuffer = MemoryFactory.allocateHeap((int)keyValuesBuffer.getCapacity(), keyValuesBuffer.getByteOrder());
        keyValuesBuffer.getBytes(spilledKeyValueBuffer.getCapacity(), spilledKeyValueBuffer.getArray());
        reset((short)0);

        return KeyValueHeapImpl.create(spilledKeyValueBuffer);
    }

    @Override
    public void insert(final byte[] key, final byte[] value)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            final int absIndex = -index - 1;
            insertAtIndex(absIndex, key, value);
        }
        else
        {
            setValue(index, value);
        }
    }

    @Override
    public void insertAtIndex(final int index, final byte[] key, final byte[] value)
    {
        assert key.length < Short.MAX_VALUE;
        assert value.length < Short.MAX_VALUE;

        assert key.length + value.length + CELL_SIZE <= keyValuesBuffer.getCapacity() - getUsedSize()
                :
                String.format("cannot insert pair due to insufficient capacity. Current Max Capacity : %d Used Capacity: %d, Required: %d",
                        keyValuesBuffer.getCapacity(),
                        getUsedSize(),
                        key.length + value.length + CELL_SIZE);

        copyKeyValueCellsWithGap(index);

        final @ByteOffset short pageRelativeOffset = appendKeyAndValue(key, value);

        keyValuesBuffer.putShort(getIndexOffset(index), pageRelativeOffset);
        keyValuesBuffer.putShort(getKeyLengthOffset(index), (short)key.length);
        keyValuesBuffer.putShort(getValueLengthOffset(index), (short)value.length);

        numberOfEntries++;

        setNumberOfPairs(numberOfEntries);
    }

    @Override
    public void setValue(final int index, final byte[] value)
    {
        final @ByteSize int newValueLength = StorageUnits.size(value.length);

        assert newValueLength < Short.MAX_VALUE;
        assert (numberOfEntries > 0 && index >= 0 && index <= (numberOfEntries - 1))
                : String.format("Index %d outside of the range of pairs [0, %d]", index, numberOfEntries - 1);

        final @ByteSize short existingValueLength = StorageUnits.size(keyValuesBuffer.getShort(getValueLengthOffset(index)));
        final @ByteSize short lengthDifferenceFromExistingPerspective = StorageUnits.size((short)(existingValueLength - newValueLength));

        assert lengthDifferenceFromExistingPerspective < keyValuesBuffer.getCapacity() - getUsedSize()
                : String.format("cannot insert pair due to insufficient capacity. Current Max Capacity : %d Used Capacity: %d, Required: %d",
                        keyValuesBuffer.getCapacity(),
                        getUsedSize(),
                        newValueLength);

        final @ByteOffset short originalOffset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
        final @ByteSize short keyLength = StorageUnits.size(keyValuesBuffer.getShort(getKeyLengthOffset(index)));
        final @ByteOffset int valueOriginalOffset = StorageUnits.offset(originalOffset + keyLength);

        if (lengthDifferenceFromExistingPerspective != 0)
        {
            final @ByteOffset int oldLogKeyValueIndexOffset = getTopHeapOffset();
            final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(getTopHeapOffset() + lengthDifferenceFromExistingPerspective);

            final @ByteSize int sizeToMove = StorageUnits.size(valueOriginalOffset - oldLogKeyValueIndexOffset);

            MemoryCopy.copy(keyValuesBuffer, oldLogKeyValueIndexOffset, keyValuesBuffer, newLogKeyValueIndexOffset, sizeToMove);

            final @ByteSize int lengthDifferenceSize = StorageUnits.size(newValueLength - existingValueLength);
            updateCell(index, ZERO_OFFSET, ZERO_SIZE, lengthDifferenceSize);
            updateCellOffsetsFromOffset(originalOffset, lengthDifferenceFromExistingPerspective);

            final @ByteOffset int newOffset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
            final @ByteOffset int newValueOffset = StorageUnits.offset(newOffset + keyLength);
            keyValuesBuffer.putBytes(newValueOffset, value);

            popBytesFromHeap(lengthDifferenceFromExistingPerspective);
        }
        else
        {
            keyValuesBuffer.putBytes(valueOriginalOffset, value);
        }
    }

    @Override
    public void putValue(final int index, final byte[] value)
    {
        final @ByteOffset short pairOffset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
        if (pairOffset == ZERO_OFFSET)
            {
                final @ByteOffset short pageRelativeOffset = appendKeyAndValue(new byte[0], value);
                keyValuesBuffer.putShort(getIndexOffset(index), pageRelativeOffset);
                keyValuesBuffer.putShort(getKeyLengthOffset(index), StorageUnits.ZERO_SHORT_SIZE);
                keyValuesBuffer.putShort(getValueLengthOffset(index), (short)value.length);
            }
    }

    private @ByteOffset short appendKeyAndValue(final byte[] key, final byte[] value)
    {
        assert key.length <= Short.MAX_VALUE : "Key size must be below " + Short.MAX_VALUE + ", provided " + key.length;
        assert value.length <= Short.MAX_VALUE : "Value size must be below " + Short.MAX_VALUE + ", provided " + value.length;

        final @ByteSize short keyValueTotalSize = StorageUnits.size((short)(key.length + value.length));
        pushBytesToHeap(keyValueTotalSize);

        final @ByteOffset short topHeapOffset = getTopHeapOffset();

        keyValuesBuffer.putBytes(topHeapOffset, key);
        keyValuesBuffer.putBytes(StorageUnits.offset(topHeapOffset + key.length), value);

        return topHeapOffset;
    }

    private void popBytesFromHeap(final @ByteSize short length)
    {
        @ByteOffset short topKeyValueHeapOffset = getTopHeapOffset();

        topKeyValueHeapOffset += StorageUnits.offset(length);

        keyValuesBuffer.putShort(TOP_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    private void pushBytesToHeap(final @ByteSize short length)
    {
        @ByteOffset short topKeyValueHeapOffset = getTopHeapOffset();

        topKeyValueHeapOffset -= StorageUnits.offset(length);

        keyValuesBuffer.putShort(TOP_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    private @ByteOffset short getTopHeapOffset()
    {
        return StorageUnits.offset(keyValuesBuffer.getShort(TOP_HEAP_SIZE_OFFSET));
    }

    private static @ByteOffset long getKeyLengthOffset(final int index)
    {
        return StorageUnits.offset(getIndexOffset(index) + CELL_PAGE_OFFSET_SIZE);
    }

    private static @ByteOffset long getValueLengthOffset(final int index)
    {
        return StorageUnits.offset(getIndexOffset(index) + CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE);
    }

    private static @ByteOffset long getIndexOffset(final int index)
    {
        return StorageUnits.offset(CELL_OFFSET + (index * CELL_SIZE));
    }

    private void setNumberOfPairs(final short numberOfLogKeyValues)
    {
        keyValuesBuffer.putShort(NUMBER_ENTRIES_OFFSET, numberOfLogKeyValues);
    }

    @Override
    public int getNumberOfPairs()
    {
        return numberOfEntries;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param gapIndex the index of the gap
     */
    private void copyKeyValueCellsWithGap(final int gapIndex)
    {
        assert gapIndex >= 0;

        final @ByteOffset long oldIndexOffset = getIndexOffset(gapIndex);
        final @ByteOffset long newIndexOffset = getIndexOffset(gapIndex + 1);

        final int numberOfEntriesToMove = numberOfEntries - gapIndex;
        final @ByteSize int size = StorageUnits.size(numberOfEntriesToMove * CELL_SIZE);
        if (size > 0)
        {
            MemoryCopy.copy(keyValuesBuffer, oldIndexOffset, keyValuesBuffer, newIndexOffset, size);
        }
    }

    @Override
    public boolean removeKeyValue(final byte[] key)
    {
        final int index = binarySearch(key);
        if (index >= 0)
        {
            removeAtIndex(index);
            return true;
        }

        return false;
    }

    @Override
    public boolean removeKeyValueAtIndex(final int index)
    {
        if (index >= 0)
        {
            removeAtIndex(index);
            return true;
        }

        return false;
    }

    private void removeAtIndex(final int index)
    {
        copyKeyValuesExcept(index);
        copyKeyValueCellsExcept(index);

        numberOfEntries--;
        setNumberOfPairs(numberOfEntries);
    }

    @Override
    public void removeOnlyKey(final int index)
    {
        final @ByteOffset short offset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(index)));
        final @ByteSize short keyLength = StorageUnits.size(keyValuesBuffer.getShort(getKeyLengthOffset(index)));

        final @ByteOffset int oldLogKeyValueIndexOffset = getTopHeapOffset();
        final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(getTopHeapOffset() + keyLength);
        final @ByteSize int sizeToMove = StorageUnits.size(offset - oldLogKeyValueIndexOffset);

        if (sizeToMove > ZERO_SIZE)
        {
            MemoryCopy.copy(keyValuesBuffer, oldLogKeyValueIndexOffset, keyValuesBuffer, newLogKeyValueIndexOffset, sizeToMove);
            updateMovedCellOffsets(offset, keyLength);
        }
        else
        {
            final @ByteOffset short newOffset = StorageUnits.offset((short) (offset + keyLength));
            keyValuesBuffer.putShort(getIndexOffset(index), newOffset);
        }

        keyValuesBuffer.putShort(getKeyLengthOffset(index), ZERO_SIZE_SHORT);

        popBytesFromHeap(keyLength);
    }

    private void updateMovedCellOffsets(final @ByteOffset short offsetMoved, final @ByteSize int totalRemoveLength)
    {
        for (int i = 0; i < numberOfEntries; i++)
        {
            final @ByteOffset long cellIndexOffset = getIndexOffset(i);
            final short currentOffset = keyValuesBuffer.getShort(cellIndexOffset);

            if (currentOffset <= offsetMoved)
            {
                keyValuesBuffer.putShort(cellIndexOffset, (short) (currentOffset + totalRemoveLength));
            }
        }
    }

    @Override
    public int binarySearch(final byte[] key)
    {
        return SearchUtils.binarySearch(key, numberOfEntries, this::getKeyAtIndex, ByteArrayComparator.INSTANCE);
    }

    private void copyKeyValuesExcept(final int removeIndex)
    {
        assert  (numberOfEntries > 0 && removeIndex <= (numberOfEntries - 1))
                : String.format("invalid index to remove %d from range [0, %d]", removeIndex, numberOfEntries - 1);

        final @ByteOffset short offset = StorageUnits.offset(keyValuesBuffer.getShort(getIndexOffset(removeIndex)));
        final @ByteSize short keyLength = StorageUnits.size(keyValuesBuffer.getShort(getKeyLengthOffset(removeIndex)));
        final @ByteSize short valueLength = StorageUnits.size(keyValuesBuffer.getShort(getValueLengthOffset(removeIndex)));
        final @ByteSize short totalRemoveLength = StorageUnits.size((short)(keyLength + valueLength));

        final @ByteOffset int oldKeyValueIndexOffset = getTopHeapOffset();
        final @ByteOffset int newKeyValueIndexOffset = StorageUnits.offset(getTopHeapOffset() + totalRemoveLength);

        final @ByteSize int sizeToMove = StorageUnits.size(offset - oldKeyValueIndexOffset);

        MemoryCopy.copy(keyValuesBuffer, oldKeyValueIndexOffset, keyValuesBuffer, newKeyValueIndexOffset, sizeToMove);

        updateCellOffsetsFromOffset(offset, totalRemoveLength);

        popBytesFromHeap(totalRemoveLength);
    }

    private void updateCellOffsetsFromOffset(final @ByteOffset short fromOffset, final @ByteSize int totalMovedLength)
    {
        for (int i = 0; i < numberOfEntries; i++)
        {
            final @ByteSize int offset = StorageUnits.size(keyValuesBuffer.getShort(getIndexOffset(i)));
            if (offset <= fromOffset)
            {
                updateCell(i, totalMovedLength, ZERO_SIZE, ZERO_SIZE);
            }
        }
    }

    private void updateCell(
            final int cellIndex,
            final @ByteSize int offsetDifference,
            final @ByteSize int keyLengthDifference,
            final @ByteSize int valueLengthDifference)
    {
        if (offsetDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellIndexOffset = getIndexOffset(cellIndex);
            final @ByteOffset short oldPairOffset = StorageUnits.offset(keyValuesBuffer.getShort(cellIndexOffset));
            final @ByteOffset short newPairOffset = StorageUnits.offset((short) (oldPairOffset + offsetDifference));
            keyValuesBuffer.putShort(cellIndexOffset, newPairOffset);
        }

        if (keyLengthDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellKeyLengthOffset = getKeyLengthOffset(cellIndex);
            final @ByteSize short oldKeyLength = StorageUnits.size(keyValuesBuffer.getShort(cellKeyLengthOffset));
            final @ByteSize short newKeyLength = StorageUnits.size((short) (oldKeyLength + keyLengthDifference));
            keyValuesBuffer.putShort(cellKeyLengthOffset, newKeyLength);
        }

        if (valueLengthDifference != ZERO_SIZE)
        {
            final @ByteOffset long cellValueLengthOffset = getValueLengthOffset(cellIndex);
            final @ByteSize short oldValueLength = StorageUnits.size(keyValuesBuffer.getShort(cellValueLengthOffset));
            final @ByteSize short newValueLength = StorageUnits.offset((short) (oldValueLength + valueLengthDifference));
            keyValuesBuffer.putShort(cellValueLengthOffset, newValueLength);
        }
    }

    private void copyKeyValueCellsExcept(final int removeIndex)
    {
        assert removeIndex >= 0;

        final int pairsToMove = numberOfEntries - removeIndex;

        assert pairsToMove >= 0;

        final @ByteOffset long oldIndexOffset = getIndexOffset(removeIndex);
        final @ByteOffset long newIndexOffset = getIndexOffset(removeIndex + 1);

        final @ByteSize int size = StorageUnits.size(pairsToMove * CELL_SIZE);
        MemoryCopy.copy(keyValuesBuffer, newIndexOffset, keyValuesBuffer, oldIndexOffset, size);
    }

    @Override
    public @ByteSize long getUsedSize()
    {
        final @ByteSize long heapSize = StorageUnits.size(keyValuesBuffer.getCapacity() - getTopHeapOffset());
        final @ByteSize long cellArraySize = StorageUnits.size(numberOfEntries * CELL_SIZE);
        return KeyValueHeapImpl.HEADER_SIZE + heapSize + cellArraySize;
    }

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder();

        if (numberOfEntries > 0)
        {
            contentBuilder.append("KVHeap : ");
            for (int i = 0; i < numberOfEntries; i++)
            {
                contentBuilder.append(new String(getKeyAtIndex(i), StandardCharsets.UTF_8));
                contentBuilder.append("-");
                contentBuilder.append(new String(getValueAtIndex(i), StandardCharsets.UTF_8));
                if (i + 1 != numberOfEntries)
                {
                    contentBuilder.append(",");
                }
            }
        }

        return contentBuilder.toString();
    }

    @Override
    public String printDebug()
    {
        final StringBuilder contentBuilder = new StringBuilder();
        if (numberOfEntries > 0)
        {
            contentBuilder.append("KVHeap : ");
            for (int i = 0; i < numberOfEntries; i++)
            {
                final byte[] keyBytes = getKeyAtIndex(i);
                final byte[] valueBytes = getValueAtIndex(i);

                final String keyLong = keyBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(keyBytes))
                        : "";

                final String valueLong = valueBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(valueBytes))
                        : "";

                contentBuilder.append(System.lineSeparator())
                        .append("Cell ").append(i)
                        .append(", offset ").append(keyValuesBuffer.getShort(getIndexOffset(i)))
                        .append(", keyLength ").append(keyValuesBuffer.getShort(getKeyLengthOffset(i)))
                        .append(", valueLength ").append(keyValuesBuffer.getShort(getValueLengthOffset(i)))
                        .append(", key ").append(new String(keyBytes, StandardCharsets.UTF_8)).append(keyLong)
                        .append(", value ").append(new String(valueBytes, StandardCharsets.UTF_8)).append(valueLong);
            }
        }

        return contentBuilder.toString();
    }
}
