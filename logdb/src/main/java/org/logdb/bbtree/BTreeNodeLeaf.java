package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public class BTreeNodeLeaf extends BTreeNodeAbstract implements BTreeNodeHeap
{
    public BTreeNodeLeaf(final @PageNumber long pageNumber, final HeapMemory memory, final int numberOfKeys)
    {
        super(pageNumber, memory, numberOfKeys, StorageUnits.offset((short) memory.getCapacity()));
    }

    @Override
    public byte[] get(final byte[] key)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            return null;
        }

        return getValue(index);
    }

    @Override
    public int getKeyIndex(final byte[] key)
    {
        final int index = binarySearch(key);
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

        destinationNode.initNodeFromBuffer();
    }

    @Override
    public void initNodeFromBuffer()
    {
        reloadCacheValuesFromBuffer();
    }

    @Override
    public void reset()
    {
        super.reset();
        entries.reset((short)0);
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getPairCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeLeaf : "when splitting a leaf node, needs same type";

        final int bNumberOfPairs = entries.getNumberOfPairs() - at;

        final BTreeNodeLeaf bTreeNodeLeaf = (BTreeNodeLeaf) splitNode;

        entries.split(at, bNumberOfPairs, bTreeNodeLeaf.entries);

        bTreeNodeLeaf.setDirty();
        setDirty();
    }

    @Override
    public void removeAtIndex(final int index)
    {
        final int keyCount = getPairCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        entries.removeKeyValueAtIndex(index);

        setDirty();
    }

    @Override
    public void insert(final byte[] key, final byte[] value)
    {
        entries.insert(key, value);
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
    public void insertChild(final int index, final byte[] key, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public void insertChild(final byte[] key, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Leaf nodes don't have children.");
    }

    @Override
    public HeapMemory getBuffer()
    {
        return (HeapMemory)buffer;
    }

    @Override
    @ByteSize long calculateFreeSpaceLeft(final @ByteSize long pageSize)
    {
        final @ByteSize long usedBytes = StorageUnits.size(BTreeNodePage.PAGE_HEADER_SIZE + entries.getUsedSize());
        return StorageUnits.size(pageSize - usedBytes);
    }
}
