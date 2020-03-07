package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;
import org.logdb.bit.DirectMemory;
import org.logdb.bit.Memory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.logdb.bbtree.BTreeNodePage.CELL_START_OFFSET;

abstract class BTreeNodeAbstract implements BTreeNode
{
    @PageNumber long pageNumber;
    boolean isDirty;
    final Memory buffer;
    final KeyValueHeap entries;

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
        this.isDirty = true;
        final Memory entriesBuffer = buffer.sliceRange(CELL_START_OFFSET, topKeyValueHeapOffset);
        this.entries = KeyValueHeapImpl.create(entriesBuffer, (short)numberOfPairs);
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

    //TODO: add a test that by reflections makes sure that all the properties are reset
    @Override
    public void reset()
    {
        pageNumber = StorageUnits.INVALID_PAGE_NUMBER;
        isDirty = false;
        buffer.reset();
    }

    void reloadCacheValuesFromBuffer()
    {
        entries.cacheNumberOfLogPairs();
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
    public int getPairCount()
    {
        return entries.getNumberOfPairs();
    }

    @Override
    public byte[] getKey(final int index)
    {
        return entries.getKeyAtIndex(index);
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
        return entries.getValueAtIndex(index);
    }

    @Override
    public byte[] getMinKey()
    {
        return entries.getKeyAtIndex(0);
    }

    @Override
    public byte[] getMaxKey()
    {
        return entries.getKeyAtIndex(entries.getNumberOfPairs() - 1);
    }

    void removeKeyAndValueWithCell(final int index, final int keyCount)
    {
        assert (keyCount - 1) >= 0
                : String.format("key size after removing index %d was %d", index, keyCount - 1);

        entries.removeKeyValueAtIndex(index);
    }

    void setValue(final int index, final long value)
    {
        setValue(index, BinaryHelper.longToBytes(value));
    }

    void setValue(final int index, final byte[] value)
    {
        // condition true only for non leaf nodes
        if (index == (entries.getNumberOfPairs() - 1) && getNodeType() == BtreeNodeType.NonLeaf)
        {
            entries.putValue(index, value);
        }

        entries.setValue(index, value);
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
        final byte[] valueR = entries.getValueAtIndex(rightmostIndex);

        bNode.setValue(0, valueR);
        removeKeyAndValueWithCell(rightmostIndex, getPairCount());

        for (int i = 0; i < bNumberOfPairs - 1; i++)
        {
            final byte[] key = entries.getKeyAtIndex(baseOffset);
            final byte[] value = entries.getValueAtIndex(baseOffset);

            bNode.insert(key, value);

            removeKeyAndValueWithCell(baseOffset, getPairCount());
        }

        final int mostRightPairIndex = aNumberOfPairs - 1;
        entries.removeOnlyKey(mostRightPairIndex);

        setDirty();
    }

    abstract @ByteSize long calculateFreeSpaceLeft(@ByteSize long pageSize);

    @Override
    public boolean shouldSplit(final @ByteSize int requiredSpace)
    {
        return requiredSpace + BTreeNodePage.CELL_SIZE >= calculateFreeSpaceLeft(buffer.getCapacity());
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

    int binarySearch(final byte[] key)
    {
        return entries.binarySearch(key);
    }

    int binarySearchNonLeaf(final byte[] key)
    {
        return SearchUtils.binarySearch(key, entries.getNumberOfPairs() - 1, this::getKey, ByteArrayComparator.INSTANCE);
    }

    @Override
    public String toString()
    {
        if (buffer instanceof DirectMemory && !((DirectMemory) buffer).isInitialized())
        {
            return "Node is Uninitialized.";
        }

        final StringBuilder contentBuilder = new StringBuilder();

        contentBuilder.append("Page:").append(pageNumber);

        if (isRoot())
        {
            contentBuilder.append(
                    String.format(" isRoot : true, previousRootPage: %d, timestamp: %d, version: %d,",
                            getPreviousRoot(),
                            getTimestamp(),
                            getVersion()));
        }

        contentBuilder.append(" keys : ");
        for (int i = 0; i < entries.getNumberOfPairs(); i++)
        {
            if (i == (entries.getNumberOfPairs() - 1) && getNodeType() == BtreeNodeType.NonLeaf)
            {
                contentBuilder.append("rightmost");
            }
            else
            {
                contentBuilder.append(new String(getKey(i), StandardCharsets.UTF_8));
                if (i + 1 != entries.getNumberOfPairs())
                {
                    contentBuilder.append(",");
                }
            }
        }
        contentBuilder.append(" values : ");
        for (int i = 0; i < entries.getNumberOfPairs(); i++)
        {
            contentBuilder.append(new String(getValue(i), StandardCharsets.UTF_8));
            if (i + 1 != entries.getNumberOfPairs())
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
        return entries.toString();
    }
}
