package org.borer.logdb;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryCopy;

import java.util.function.LongSupplier;

public class BTreeNodeLeaf extends BTreeNodeAbstract
{
    /**
     * split constructor.
     */
    public BTreeNodeLeaf(
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues,
            final LongSupplier idSupplier)
    {
        super(memory, numberOfKeys, numberOfValues, idSupplier);
    }

    /**
     * Copy constructor.
     */
    private BTreeNodeLeaf(
            final long id,
            final Memory memory,
            final int numberOfKeys,
            final int numberOfValues,
            final LongSupplier idSupplier)
    {
        super(id, memory, numberOfKeys, numberOfValues, idSupplier);
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
    public BTreeNode copy(final Memory memoryForCopy)
    {
        MemoryCopy.copy(buffer, memoryForCopy);

        return new BTreeNodeLeaf(getId(), memoryForCopy, numberOfKeys, numberOfValues, idSupplier);
    }

    @Override
    public BTreeNode split(final int at, final Memory memoryForNewNode)
    {
        final int keyCount = getKeyCount();
        if (keyCount <= 0)
        {
            return null;
        }

        final int bNumberOfKeys = numberOfKeys - at;
        final int bNumberOfValues = numberOfValues - at;

        //TODO: allocate from other place
        final BTreeNodeLeaf bTreeNodeLeaf = new BTreeNodeLeaf(
                memoryForNewNode,
                bNumberOfKeys,
                bNumberOfValues,
                idSupplier);
        bTreeNodeLeaf.updateNumberOfKeys(bTreeNodeLeaf.numberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bTreeNodeLeaf.numberOfValues);

        splitKeys(at, bNumberOfKeys,  bTreeNodeLeaf);
        splitValues(at, bNumberOfValues, bTreeNodeLeaf);

        return bTreeNodeLeaf;
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
}
