package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class BTreeNodeLeaf extends BTreeNodeAbstract implements BTreeNodeHeap
{
    public BTreeNodeLeaf(final @PageNumber long pageNumber, final HeapMemory memory, final int numberOfKeys)
    {
        super(pageNumber, memory, numberOfKeys, StorageUnits.offset((short) memory.getCapacity()));
    }

    @Override
    public long get(final long key)
    {
        return BinaryHelper.bytesToLong(get(BinaryHelper.longToBytes(key)));
    }

    public byte[] get(final byte[] key)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            return BinaryHelper.longToBytes(KEY_NOT_FOUND_VALUE);
        }

        return getValueBytes(index);
    }

    @Override
    public int getKeyIndex(final long key)
    {
        final int index = binarySearch(BinaryHelper.longToBytes(key));
        if (index < 0)
        {
            //TODO: make this return KEY_NOT_FOUND_VALUE instead of min_value
            return Integer.MIN_VALUE;
        }

        return index;
    }

    @Override
    public void copy(final BTreeNodeHeap destinationNode)
    {
        assert destinationNode instanceof BTreeNodeLeaf : "when coping a leaf node, needs same type";

        MemoryCopy.copy(buffer, destinationNode.getBuffer());

        destinationNode.getBuffer().putShort(BTreeNodePage.TOP_KEY_VALUES_HEAP_SIZE_OFFSET, topKeyValueHeapOffset);
        destinationNode.initNodeFromBuffer();
    }

    @Override
    public void initNodeFromBuffer()
    {
        reloadCacheValuesFromBuffer();
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getPairCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeLeaf : "when splitting a leaf node, needs same type";

        final int bNumberOfPairs = numberOfPairs - at;

        final BTreeNodeLeaf bTreeNodeLeaf = (BTreeNodeLeaf) splitNode;

        splitKeysAndValues(at, bNumberOfPairs, bTreeNodeLeaf);

        bTreeNodeLeaf.setDirty();
        setDirty();
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getPairCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKeyAndValueWithCell(index, keyCount);

        setDirty();
    }

    @Override
    public void insert(final long key, final long value)
    {
        insert(BinaryHelper.longToBytes(key), BinaryHelper.longToBytes(value));
    }

    @Override
    public void insert(final byte[] key, final byte[] value)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            final int absIndex = -index - 1;
            insertKeyAndValue(absIndex, key, value);
        }
        else
        {
            setValue(index, value);
        }

        setDirty();
    }

    @Override
    public @PageNumber long commit(
            final NodesManager nodesManager,
            final boolean isRoot,
            final @PageNumber long previousRootPageNumber,
            final @Milliseconds long timestamp,
            final @Version long version) throws IOException
    {
        if (isDirty)
        {
            preCommit(isRoot, previousRootPageNumber, timestamp, version);
            this.pageNumber = nodesManager.commitNode(this);
        }

        return this.pageNumber;
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.Leaf;
    }

    @Override
    public BTreeNode getChildAt(int index)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public void insertChild(final int index, final long key, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public boolean shouldSplit(int requiredSpace)
    {
        final int minimumFreeSpaceBeforeOperatingOnNode = 2 * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        return minimumFreeSpaceBeforeOperatingOnNode > freeSizeLeftBytes;
    }

    @Override
    public HeapMemory getBuffer()
    {
        return (HeapMemory)buffer;
    }

    @Override
    @ByteSize long calculateFreeSpaceLeft(final long pageSize)
    {
        final @ByteSize int sizeForKeyValuesCells = StorageUnits.size(numberOfPairs * BTreeNodePage.CELL_SIZE);
        final @ByteSize long logHeapSize = StorageUnits.size(pageSize - topKeyValueHeapOffset);
        final long usedBytes = BTreeNodePage.HEADER_SIZE_BYTES + sizeForKeyValuesCells + logHeapSize;
        return StorageUnits.size(pageSize - usedBytes);
    }
}
