package org.logdb.bbtree;

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

import java.util.Objects;

abstract class BTreeNodeAbstract implements BTreeNode
{
    long freeSizeLeftBytes;

    @PageNumber long pageNumber;
    int numberOfKeys;
    int numberOfValues;
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
                      final int numberOfKeys,
                      final int numberOfValues)
    {
        this.pageNumber = pageNumber;
        this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
        this.numberOfKeys = numberOfKeys;
        this.numberOfValues = numberOfValues;
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
        numberOfValues = getNodeType() == BtreeNodeType.NonLeaf ? 1 : 0;
        isDirty = false;
        buffer.reset();
    }

    private void setNodePageType(final BtreeNodeType type)
    {
        buffer.putByte(BTreeNodePage.PAGE_TYPE_OFFSET, type.getType());
    }

    private void setRootFlag(final boolean isRoot)
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
    public int getKeyCount()
    {
        return numberOfKeys;
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

    boolean logHasFreeSpace()
    {
        return freeSizeLeftBytes > (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
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

    void recalculateFreeSpaceLeft()
    {
        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    abstract long calculateFreeSpaceLeft(long pageSize);

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

        final int extraValues = numberOfValues - numberOfKeys;
        final int pairsToMove = numberOfKeys - removeIndex;

        assert extraValues >= 0;
        assert pairsToMove >= 0;

        final @ByteOffset long oldIndexOffset = getKeyIndexOffset(removeIndex);
        final @ByteOffset long newIndexOffset = getKeyIndexOffset(removeIndex + 1);

        final @ByteSize int size =
                StorageUnits.size((pairsToMove + extraValues) * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE));
        MemoryCopy.copy(buffer, newIndexOffset, buffer, oldIndexOffset, size);
    }

    int binarySearch(final long key)
    {
        return SearchUtils.binarySearch(key, numberOfKeys, this::getKey);
    }

    @Override
    public String toString()
    {
        if (buffer instanceof DirectMemory && ((DirectMemory) buffer).isUninitialized())
        {
            return "Node is Uninitialized.";
        }

        final StringBuilder contentBuilder = new StringBuilder();

        if (isRoot())
        {
            contentBuilder.append(
                    String.format(" isRoot : true, previousRoot: %d, timestamp: %d, version: %d,",
                            getPreviousRoot(),
                            getTimestamp(),
                            getVersion()));
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
