package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;
import org.logdb.bit.DirectMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.charset.Charset;
import java.util.Objects;

import static org.logdb.bbtree.BTreeNodePage.CELL_KEY_LENGTH_SIZE;
import static org.logdb.bbtree.BTreeNodePage.CELL_PAGE_OFFSET_SIZE;
import static org.logdb.bbtree.BTreeNodePage.CELL_SIZE;
import static org.logdb.bbtree.BTreeNodePage.TOP_KEY_VALUES_HEAP_SIZE_OFFSET;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_SIZE_SHORT;

abstract class BTreeNodeAbstract implements BTreeNode
{
    @ByteOffset short topKeyValueHeapOffset;

    @ByteSize long freeSizeLeftBytes;
    @PageNumber long pageNumber;
    int numberOfPairs;
    boolean isDirty;
    final Memory buffer;

    /**
     * Constructor.
     *
     * @param pageNumber     the page number of this node or an id generated for not yet persisted nodes
     * @param buffer         the buffer used as a content for this node
     * @param numberOfPairs   number of pairs in this node
     */
    BTreeNodeAbstract(final @PageNumber long pageNumber,
                      final Memory buffer,
                      final int numberOfPairs,
                      final @ByteOffset short topKeyValueHeapOffset)
    {
        this.pageNumber = pageNumber;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.numberOfPairs = numberOfPairs;
        this.topKeyValueHeapOffset = topKeyValueHeapOffset;
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
        setTopKeyValueHeapOffset();
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
        setTopKeyValueHeapOffset();
        isDirty = false;
    }

    private void popBytesFromKeyValueHeap(final @ByteSize short length)
    {
        topKeyValueHeapOffset += StorageUnits.offset(length);
        buffer.putShort(TOP_KEY_VALUES_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    private void pushBytesToKeyValueHeap(final @ByteSize short length)
    {
        topKeyValueHeapOffset -= StorageUnits.offset(length);
        buffer.putShort(TOP_KEY_VALUES_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
    }

    //TODO: add a test that by reflections makes sure that all the properties are reset
    @Override
    public void reset()
    {
        freeSizeLeftBytes = StorageUnits.ZERO_SIZE;
        pageNumber = StorageUnits.INVALID_PAGE_NUMBER;
        numberOfPairs = getNodeType() == BtreeNodeType.NonLeaf ? 1 : 0;
        isDirty = false;
        topKeyValueHeapOffset = StorageUnits.offset((short) buffer.getCapacity());
        buffer.reset();
    }

    void reloadCacheValuesFromBuffer()
    {
        numberOfPairs = buffer.getInt(BTreeNodePage.NUMBER_OF_PAIRS_OFFSET);
        topKeyValueHeapOffset = StorageUnits.offset(buffer.getShort(BTreeNodePage.TOP_KEY_VALUES_HEAP_SIZE_OFFSET));

        recalculateFreeSpaceLeft();
    }

    private void setNodePageType(final BtreeNodeType type)
    {
        buffer.putByte(BTreeNodePage.PAGE_TYPE_OFFSET, type.getType());
    }

    private void setRootFlag(final boolean isRoot)
    {
        buffer.putByte(BTreeNodePage.PAGE_IS_ROOT_OFFSET, (byte) (isRoot ? 1 : 0));
    }

    private void setTopKeyValueHeapOffset()
    {
        buffer.putShort(BTreeNodePage.TOP_KEY_VALUES_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
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

    private void setPreviousRoot(final @PageNumber long previousRootPageNumber)
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

    @Override
    public int getPairCount()
    {
        return numberOfPairs;
    }

    @Override
    public byte[] getKey(final int index)
    {
        final @ByteOffset short keyOffset = StorageUnits.offset(buffer.getShort(getCellIndexOffset(index)));
        final @ByteSize short keyLength = StorageUnits.size(buffer.getShort(getCellIndexKeyLength(index)));
        final byte[] keyBytes = new byte[keyLength];

        buffer.getBytes(keyOffset, keyLength, keyBytes);

        return keyBytes;
    }

    /**
     * Gets the value at index position.
     *
     * @param index Index inside the btree leaf
     * @return The value or null if it doesn't exist
     */
    @Override
    public byte[] getValue(final int index)
    {
        final short keyOffset = buffer.getShort(getCellIndexOffset(index));
        final short keyLength = buffer.getShort(getCellIndexKeyLength(index));
        final @ByteSize short valueLength = StorageUnits.size(buffer.getShort(getCellIndexValueLength(index)));
        final @ByteOffset int valueOffset = StorageUnits.offset(keyOffset + keyLength);

        final byte[] valueBytes = new byte[valueLength];

        buffer.getBytes(valueOffset, valueLength, valueBytes);

        return valueBytes;
    }

    @Override
    public byte[] getMinKey()
    {
        return getKey(0);
    }

    @Override
    public byte[] getMaxKey()
    {
        return getKey(numberOfPairs - 1);
    }

    private static @ByteOffset long getCellIndexKeyLength(final int index)
    {
        return StorageUnits.offset(getCellIndexOffset(index) + CELL_PAGE_OFFSET_SIZE);
    }

    private static @ByteOffset long getCellIndexValueLength(final int index)
    {
        return StorageUnits.offset(getCellIndexOffset(index) + CELL_PAGE_OFFSET_SIZE + CELL_KEY_LENGTH_SIZE);
    }

    private static @ByteOffset long getCellIndexOffset(final int index)
    {
        return StorageUnits.offset(StorageUnits.offset(BTreeNodePage.CELL_START_OFFSET + (index * CELL_SIZE)));
    }

    void insertKeyAndValue(final int index, final byte[] key, final byte[] value)
    {
        final int keyCount = getPairCount();
        assert index <= keyCount
                : String.format("index to insert %d > node key cound %d ", index, keyCount);

        copyKeyValueCellsWithGap(index);

        final @ByteOffset short pageRelativeOffset = appendKeyAndValue(key, value);

        buffer.putShort(getCellIndexOffset(index), pageRelativeOffset);
        buffer.putShort(getCellIndexKeyLength(index), (short)key.length);
        buffer.putShort(getCellIndexValueLength(index), (short)value.length);

        numberOfPairs++;
        updateNumberOfPairs(numberOfPairs);

        recalculateFreeSpaceLeft();
    }

    private @ByteOffset short appendKeyAndValue(final byte[] key, final byte[] value)
    {
        assert key.length <= Short.MAX_VALUE : "Key size must be below " + Short.MAX_VALUE + ", provided " + key.length;
        assert value.length <= Short.MAX_VALUE : "Value size must be below " + Short.MAX_VALUE + ", provided " + value.length;
        assert key.length + value.length <= Short.MAX_VALUE : "Key and value total size must be less than " + Short.MAX_VALUE;

        final @ByteSize short keyValueTotalSize = StorageUnits.size((short)(key.length + value.length));
        pushBytesToKeyValueHeap(keyValueTotalSize);

        buffer.putBytes(topKeyValueHeapOffset, key);
        buffer.putBytes(StorageUnits.offset(topKeyValueHeapOffset + key.length), value);

        return topKeyValueHeapOffset;
    }

    private @ByteOffset short appendValue(final byte[] value)
    {
        assert value.length <= Short.MAX_VALUE : "Value size must be below " + Short.MAX_VALUE + ", provided " + value.length;

        final @ByteSize short valueSize = StorageUnits.size((short)value.length);
        pushBytesToKeyValueHeap(valueSize);

        buffer.putBytes(topKeyValueHeapOffset, value);

        return topKeyValueHeapOffset;
    }

    void removeKeyAndValueWithCell(final int index, final int keyCount)
    {
        assert (keyCount - 1) >= 0
                : String.format("key size after removing index %d was %d", index, keyCount - 1);

        copyKeyValuesExcept(index);

        copyKeyValueCellsExcept(index);

        numberOfPairs--;
        updateNumberOfPairs(numberOfPairs);

        recalculateFreeSpaceLeft();
    }

    void updateNumberOfPairs(final int numberOfPairs)
    {
        this.numberOfPairs = numberOfPairs;
        buffer.putInt(BTreeNodePage.NUMBER_OF_PAIRS_OFFSET, numberOfPairs);
    }

    void setValue(final int index, final long value)
    {
        setValue(index, BinaryHelper.longToBytes(value));
    }

    void setValue(final int index, final byte[] value)
    {
        // condition true only for non leaf nodes
        if (index == (numberOfPairs - 1) && getNodeType() == BtreeNodeType.NonLeaf)
        {
            final short pairOffset = buffer.getShort(getCellIndexOffset(index));

            if (pairOffset == ZERO_OFFSET)
            {
                final @ByteOffset short pageRelativeOffset = appendValue(value);
                buffer.putShort(getCellIndexOffset(index), pageRelativeOffset);
                buffer.putShort(getCellIndexKeyLength(index), StorageUnits.ZERO_SHORT_SIZE);
                buffer.putShort(getCellIndexValueLength(index), (short)value.length);

                recalculateFreeSpaceLeft();
            }
        }

        final short pairOffset = buffer.getShort(getCellIndexOffset(index));
        final short keyLength = buffer.getShort(getCellIndexKeyLength(index));
        final @ByteOffset long valueIndexOffset = StorageUnits.offset(pairOffset + keyLength);

        buffer.putBytes(valueIndexOffset, value);
    }

    void splitKeysAndValues(
            final int aNumberOfPairs,
            final int bNumberOfPairs,
            final BTreeNodeAbstract bNode)
    {
        final int index = aNumberOfPairs;
        for (int i = 0; i < bNumberOfPairs; i++)
        {
            final @ByteOffset short offset = StorageUnits.offset(buffer.getShort(getCellIndexOffset(index)));
            final @ByteSize short keyLength = StorageUnits.size(buffer.getShort(getCellIndexKeyLength(index)));
            final @ByteSize short valueLength = StorageUnits.offset(buffer.getShort(getCellIndexValueLength(index)));

            final byte[] key = new byte[keyLength];
            final byte[] value = new byte[valueLength];

            buffer.getBytes(offset, keyLength, key);
            buffer.getBytes(StorageUnits.offset(offset + keyLength), valueLength, value);

            bNode.insert(key, value);

            removeAtIndex(index);
        }

        recalculateFreeSpaceLeft();
        bNode.recalculateFreeSpaceLeft();
    }

    void splitKeysAndValuesNonLeaf(
            final int aNumberOfPairs,
            final int bNumberOfPairs,
            final BTreeNodeAbstract bNode)
    {
        //copy keys
        final int baseOffset = aNumberOfPairs;

        //////set bnode rightmost value
        final int rightmostIndex = (baseOffset + bNumberOfPairs) - 1;
        final @ByteOffset short offsetR = StorageUnits.offset(buffer.getShort(getCellIndexOffset(rightmostIndex)));
        final @ByteSize short valueLengthR = StorageUnits.size(buffer.getShort(getCellIndexValueLength(rightmostIndex)));
        final byte[] valueR = new byte[valueLengthR];
        buffer.getBytes(offsetR, valueLengthR, valueR);

        bNode.setValue(0, valueR);
        removeKeyAndValueWithCell(rightmostIndex, getPairCount());

        for (int i = 0; i < bNumberOfPairs - 1; i++)
        {
            final @ByteOffset short offset = StorageUnits.offset(buffer.getShort(getCellIndexOffset(baseOffset)));
            final @ByteSize short keyLength = StorageUnits.size(buffer.getShort(getCellIndexKeyLength(baseOffset)));
            final @ByteSize short valueLength = StorageUnits.size(buffer.getShort(getCellIndexValueLength(baseOffset)));

            final byte[] key = new byte[keyLength];
            final byte[] value = new byte[valueLength];

            buffer.getBytes(offset, keyLength, key);
            buffer.getBytes(StorageUnits.offset(offset + keyLength), valueLength, value);

            bNode.insert(key, value);

            removeKeyAndValueWithCell(baseOffset, getPairCount());
        }

        final int mostRightPairIndex = aNumberOfPairs - 1;
        final @ByteOffset short offset = StorageUnits.offset(buffer.getShort(getCellIndexOffset(mostRightPairIndex)));
        final @ByteSize short keyLength = StorageUnits.size(buffer.getShort(getCellIndexKeyLength(mostRightPairIndex)));

        final @ByteOffset int oldLogKeyValueIndexOffset = topKeyValueHeapOffset;
        final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(topKeyValueHeapOffset + keyLength);
        final @ByteSize int sizeToMove = StorageUnits.size(offset - oldLogKeyValueIndexOffset);

        if (sizeToMove > ZERO_SIZE)
        {
            MemoryCopy.copy(buffer, oldLogKeyValueIndexOffset, buffer, newLogKeyValueIndexOffset, sizeToMove);
            updateMovedCellOffsets(offset, keyLength);
        }
        else
        {
            final @ByteOffset short newOffset = StorageUnits.offset((short) (offset + keyLength));
            buffer.putShort(getCellIndexOffset(mostRightPairIndex), newOffset);
        }

        buffer.putShort(getCellIndexKeyLength(mostRightPairIndex), ZERO_SIZE_SHORT);

        popBytesFromKeyValueHeap(keyLength);

        numberOfPairs = aNumberOfPairs;
        updateNumberOfPairs(numberOfPairs);

        recalculateFreeSpaceLeft();
        bNode.recalculateFreeSpaceLeft();

        setDirty();
    }

    void recalculateFreeSpaceLeft()
    {
        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    abstract @ByteSize long calculateFreeSpaceLeft(long pageSize);

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
    private void copyKeyValueCellsWithGap(final int gapIndex)
    {
        assert gapIndex >= 0;

        final int pairsToMove = numberOfPairs - gapIndex;

        assert pairsToMove >= 0;

        final @ByteOffset long oldIndexOffset = getCellIndexOffset(gapIndex);
        final @ByteOffset long newIndexOffset = getCellIndexOffset(gapIndex + 1);

        final @ByteSize int size = StorageUnits.size(pairsToMove * CELL_SIZE);
        MemoryCopy.copy(buffer, oldIndexOffset, buffer, newIndexOffset, size);
    }

    private void copyKeyValueCellsExcept(final int removeIndex)
    {
        assert removeIndex >= 0;

        final int pairsToMove = numberOfPairs - removeIndex - 1;

        assert pairsToMove >= 0;

        final @ByteOffset long oldIndexOffset = getCellIndexOffset(removeIndex);
        final @ByteOffset long newIndexOffset = getCellIndexOffset(removeIndex + 1);

        final @ByteSize int size = StorageUnits.size(pairsToMove * CELL_SIZE);
        MemoryCopy.copy(buffer, newIndexOffset, buffer, oldIndexOffset, size);
    }

    private void copyKeyValuesExcept(final int removeIndex)
    {
        assert  (numberOfPairs > 0 && removeIndex <= (numberOfPairs - 1))
                : String.format("invalid index to remove %d from range [0, %d]", removeIndex, numberOfPairs - 1);

        final @ByteOffset short offset = StorageUnits.offset(buffer.getShort(getCellIndexOffset(removeIndex)));
        final @ByteSize short keyLength = StorageUnits.size(buffer.getShort(getCellIndexKeyLength(removeIndex)));
        final @ByteSize short valueLength = StorageUnits.size(buffer.getShort(getCellIndexValueLength(removeIndex)));

        assert (short)(keyLength + valueLength) < Short.MAX_VALUE;

        final @ByteSize short totalRemoveLength = StorageUnits.size((short)(keyLength + valueLength));

        final @ByteOffset int oldLogKeyValueIndexOffset = topKeyValueHeapOffset;
        final @ByteOffset int newLogKeyValueIndexOffset = StorageUnits.offset(topKeyValueHeapOffset + totalRemoveLength);

        final @ByteSize int sizeToMove = StorageUnits.size(offset - oldLogKeyValueIndexOffset);

        if (sizeToMove != 0)
        {
            MemoryCopy.copy(buffer, oldLogKeyValueIndexOffset, buffer, newLogKeyValueIndexOffset, sizeToMove);
            updateMovedCellOffsets(offset, totalRemoveLength);
        }

        popBytesFromKeyValueHeap(totalRemoveLength);
    }

    private void updateMovedCellOffsets(final @ByteOffset short offsetMoved, final @ByteSize int totalRemoveLength)
    {
        for (int i = 0; i < numberOfPairs; i++)
        {
            final @ByteOffset long cellIndexOffset = getCellIndexOffset(i);
            final short currentOffset = buffer.getShort(cellIndexOffset);

            if (currentOffset <= offsetMoved)
            {
                buffer.putShort(cellIndexOffset, (short) (currentOffset + totalRemoveLength));
            }
        }
    }

    int binarySearch(final byte[] key)
    {
        return SearchUtils.binarySearch(key, numberOfPairs, this::getKey, ByteArrayComparator.INSTANCE);
    }

    int binarySearchNonLeaf(final byte[] key)
    {
        return SearchUtils.binarySearch(key, numberOfPairs - 1, this::getKey, ByteArrayComparator.INSTANCE);
    }

    @Override
    public String toString()
    {
        if (buffer instanceof DirectMemory && !((DirectMemory) buffer).isInitialized())
        {
            return "Node is Uninitialized.";
        }

        final StringBuilder contentBuilder = new StringBuilder();
        Charset utf8 = Charset.forName("UTF-8");

        if (isRoot())
        {
            contentBuilder.append(
                    String.format(" isRoot : true, previousRoot: %d, timestamp: %d, version: %d,",
                            getPreviousRoot(),
                            getTimestamp(),
                            getVersion()));
        }

        contentBuilder.append(" keys : ");
        for (int i = 0; i < numberOfPairs; i++)
        {
            if (i == (numberOfPairs - 1) && getNodeType() == BtreeNodeType.NonLeaf)
            {
                contentBuilder.append("rightmost");
            }
            else
            {
                contentBuilder.append(new String(getKey(i), utf8));
                if (i + 1 != numberOfPairs)
                {
                    contentBuilder.append(",");
                }
            }
        }
        contentBuilder.append(" values : ");
        for (int i = 0; i < numberOfPairs; i++)
        {
            contentBuilder.append(new String(getValue(i), utf8));
            if (i + 1 != numberOfPairs)
            {
                contentBuilder.append(",");
            }
        }

        return String.format("%s{ %s }",
                getNodeType().name(),
                contentBuilder.toString());
    }

    String printDebug()
    {
        final StringBuilder contentBuilder = new StringBuilder();

        if (numberOfPairs > 0)
        {
            Charset utf8 = Charset.forName("UTF-8");
            contentBuilder.append("KeyValueEntries : ");
            for (int i = 0; i < numberOfPairs; i++)
            {
                final byte[] keyBytes = getKey(i);
                final byte[] valueBytes = getValue(i);

                final String keyLong = keyBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(keyBytes))
                        : "";

                final String valueLong = valueBytes.length == LONG_BYTES_SIZE
                        ? String.format("(%d)", BinaryHelper.bytesToLong(valueBytes))
                        : "";

                contentBuilder.append(System.lineSeparator())
                        .append("Cell ").append(i)
                        .append(", offset ").append(buffer.getShort(getCellIndexOffset(i)))
                        .append(", keyLength ").append(buffer.getShort(getCellIndexKeyLength(i)))
                        .append(", valueLength ").append(buffer.getShort(getCellIndexValueLength(i)))
                        .append(", key ").append(new String(keyBytes, utf8)).append(keyLong)
                        .append(", value ").append(new String(valueBytes, utf8)).append(valueLong);
            }
        }

        return contentBuilder.toString();
    }
}
