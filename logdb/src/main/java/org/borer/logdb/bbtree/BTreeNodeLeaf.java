package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryCopy;
import org.borer.logdb.storage.NodesManager;

public class BTreeNodeLeaf extends BTreeNodeAbstract
{
    /**
     * Load constructor.
     */
    public BTreeNodeLeaf(final long pageNumber, final Memory memory)
    {
        super(pageNumber, memory);
    }


    /**
     * Copy/Split constructor.
     */
    public BTreeNodeLeaf(
            final long pageNumber,
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues)
    {
        super(pageNumber, memory, numberOfKeys, numberOfValues);
    }

    @Override
    public long get(final long key)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            return -1;
        }

        return getValue(index);
    }

    @Override
    public void copy(final BTreeNode copyNode)
    {
        assert copyNode instanceof BTreeNodeLeaf : "when splitting a leaf node, needs same type";

        final BTreeNodeLeaf bTreeNodeLeaf = (BTreeNodeLeaf) copyNode;
        MemoryCopy.copy(buffer, bTreeNodeLeaf.getBuffer());

        bTreeNodeLeaf.updateBuffer(bTreeNodeLeaf.getBuffer());
    }

    @Override
    public void split(final int at, final BTreeNode splitNode)
    {
        assert getKeyCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeLeaf : "when splitting a leaf node, needs same type";

        final int bNumberOfKeys = numberOfKeys - at;
        final int bNumberOfValues = numberOfValues - at;

        final BTreeNodeLeaf bTreeNodeLeaf = (BTreeNodeLeaf) splitNode;
        bTreeNodeLeaf.updateNumberOfKeys(bNumberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bNumberOfValues);

        splitKeys(at, bNumberOfKeys,  bTreeNodeLeaf);
        splitValues(at, bNumberOfValues, bTreeNodeLeaf);

        setDirty();
    }

    @Override
    public boolean needRebalancing(final int threshold)
    {
        return numberOfKeys == 0 || numberOfKeys < threshold;
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount > index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKey(index, keyCount);
        removeValue(index, keyCount);
    }

    @Override
    public void insert(final long key, final long value)
    {
        final int index = binarySearch(key);

        if (index < 0)
        {
            final int absIndex = -index - 1;
            insertKey(absIndex, key);
            insertValue(absIndex, value);
        }
        else
        {
            setValue(index, value);
        }
    }

    @Override
    public long commit(final NodesManager nodesManager)
    {
        if (isDirty)
        {
            preCommit();
            pageNumber = nodesManager.commitNode(this);
            isDirty = false;
        }

        return pageNumber;
    }

    @Override
    protected BtreeNodeType getNodeType()
    {
        return BtreeNodeType.Leaf;
    }
}
