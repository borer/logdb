package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.NodesManager;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_INDEX_NOT_FOUND;
import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class BTreeNodeLeaf extends BTreeNodeAbstract implements BTreeNodeHeap
{
    /**
     * Load constructor.
     */
    public BTreeNodeLeaf(final @PageNumber long pageNumber, final HeapMemory memory)
    {
        super(pageNumber, memory);
    }


    /**
     * Copy/Split constructor.
     */
    public BTreeNodeLeaf(
            final @PageNumber long pageNumber,
            final HeapMemory memory,
            final int numberOfLogKeyValues,
            final int numberOfKeys,
            final int numberOfValues)
    {
        super(pageNumber, memory, numberOfLogKeyValues, numberOfKeys, numberOfValues);
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
    public int getKeyIndex(long key)
    {
        final int index = binarySearch(key);
        if (index < 0)
        {
            return KEY_INDEX_NOT_FOUND;
        }

        return index;
    }

    @Override
    public void copy(final BTreeNodeHeap copyNode)
    {
        MemoryCopy.copy(buffer, copyNode.getBuffer());
        copyNode.initNodeFromBuffer();
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
            final @Version long version)
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
    public HeapMemory getBuffer()
    {
        return (HeapMemory)buffer;
    }
}
