package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class BTreeNodeLeaf extends BTreeNodeAbstract implements BTreeNodeHeap
{
    public BTreeNodeLeaf(
            final @PageNumber long pageNumber,
            final HeapMemory memory,
            final int numberOfKeys,
            final int numberOfValues)
    {
        super(pageNumber, memory, numberOfKeys, numberOfValues);
    }

    public static BTreeNodeLeaf load(final @PageNumber long pageNumber, final HeapMemory memory)
    {
        return new BTreeNodeLeaf(
                pageNumber,
                memory,
                memory.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET),
                memory.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET));
    }

    @Override
    public long get(final long key)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            return KEY_NOT_FOUND_VALUE;
        }

        return getValue(index);
    }

    @Override
    public int getKeyIndex(final long key)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            return Integer.MIN_VALUE;
        }

        return index;
    }

    @Override
    public void copy(final BTreeNodeHeap destinationNode)
    {
        assert destinationNode instanceof BTreeNodeLeaf : "when coping a leaf node, needs same type";

        MemoryCopy.copy(buffer, destinationNode.getBuffer());
        destinationNode.initNodeFromBuffer();
    }

    @Override
    public void initNodeFromBuffer()
    {
        numberOfKeys = buffer.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET);
        numberOfValues = buffer.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET);
        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getKeyCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeLeaf : "when splitting a leaf node, needs same type";

        final int bNumberOfKeys = numberOfKeys - at;
        final int bNumberOfValues = numberOfValues - at;

        final BTreeNodeLeaf bTreeNodeLeaf = (BTreeNodeLeaf) splitNode;
        bTreeNodeLeaf.updateNumberOfKeys(bNumberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bNumberOfValues);

        splitKeysAndValues(at, bNumberOfKeys, bTreeNodeLeaf);

        bTreeNodeLeaf.setDirty();
        setDirty();
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKeyAndValue(index, keyCount);

        setDirty();
    }

    @Override
    public void insert(final long key, final long value)
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
    public int getNumberOfChildren()
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public boolean shouldSplit()
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
    long calculateFreeSpaceLeft(long pageSize)
    {
        final int sizeForKeyValues = numberOfKeys * (BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        final int usedBytes = sizeForKeyValues + BTreeNodePage.HEADER_SIZE_BYTES;
        return pageSize - usedBytes;
    }
}
