package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public class BTreeNodeNonLeaf extends BTreeLogNodeAbstract implements BTreeNodeHeap
{
    static final byte[] NON_COMMITTED_CHILD = BinaryHelper.longToBytes(Long.MIN_VALUE);

    private BTreeNodeHeap[] children;

    public BTreeNodeNonLeaf(
            final @PageNumber long pageNumber,
            final HeapMemory memory,
            final @ByteSize int maxLogSize,
            final int numberOfPairs,
            final BTreeNodeHeap[] children)
    {
        super(pageNumber, memory, maxLogSize, numberOfPairs);
        this.children = children;
    }

    @Override
    public void insert(final byte[] key, final byte[] value)
    {
        final int index = binarySearchNonLeaf(key);
        if (index < 0)
        {
            final int absIndex = -index - 1;
            entries.insertAtIndex(absIndex, key, value);
        }
        else
        {
            setValue(index, value);
        }

        setDirty();
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        //Note: this sentinel value will be replaced with the actual page number once we commit the child page
        setValue(index, NON_COMMITTED_CHILD);

        children[index] = child;

        setDirty();
    }

    @Override
    public BTreeNode getChildAt(final int index)
    {
        return children[index];
    }

    @Override
    public void insertChild(final int index, final byte[] key, final BTreeNodeHeap child)
    {
        //this will be replaced once we commit the child page
        entries.insertAtIndex(index, key, NON_COMMITTED_CHILD);
        final int oldNumberOfPairs = entries.getNumberOfPairs() - 1;

        BTreeNodeHeap[] newChildren = new BTreeNodeHeap[entries.getNumberOfPairs()];
        copyWithGap(children, newChildren, oldNumberOfPairs, index);
        children = newChildren;
        children[index] = child;

        setDirty();
    }

    @Override
    public void insertChild(final byte[] key, final BTreeNodeHeap child)
    {
        int index = binarySearchNonLeaf(key);
        if (index < 0)
        {
            index  = -index - 1;
        }

        insertChild(index, key, child);
    }

    @Override
    public void removeAtIndex(final int index)
    {
        final int keyCount = getPairCount();

        assert keyCount >= index && keyCount > 0 && index >= 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        entries.removeKeyValueAtIndex(index);

        //TODO: when removing children consider returning them to the cache
        removeChildReference(index);

        setDirty();
    }

    private void removeChildReference(final int index)
    {
        final int oldChildrenSize = children.length;
        final int newChildrenSize = oldChildrenSize - 1;
        assert newChildrenSize >= 0
                : String.format("children size after removing index %d was %d", index, newChildrenSize);
        final BTreeNodeHeap[] newChildren = new BTreeNodeHeap[newChildrenSize];
        copyExcept(children, newChildren, oldChildrenSize, index);
        children = newChildren;
    }

    @Override
    public byte[] get(final byte[] key)
    {
        int index = getKeyIndex(key);
        return getValue(index);
    }

    @Override
    public int getKeyIndex(final byte[] key)
    {
        int index = binarySearchNonLeaf(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return index;
    }

    @Override
    public void copy(final BTreeNodeHeap destinationNode)
    {
        assert destinationNode instanceof BTreeNodeNonLeaf : "when copying a non leaf node, needs same type";

        MemoryCopy.copy(buffer, destinationNode.getBuffer());

        destinationNode.initNodeFromBuffer();

        final BTreeNodeHeap[] copyChildren = new BTreeNodeHeap[children.length];
        System.arraycopy(children, 0, copyChildren, 0, children.length);

        final BTreeNodeNonLeaf bTreeNodeNonLeaf = (BTreeNodeNonLeaf) destinationNode;
        bTreeNodeNonLeaf.setChildren(copyChildren);
    }

    @Override
    public void initNodeFromBuffer()
    {
        reloadCacheValuesFromBuffer();
        refreshKeyValueLog();
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getPairCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeNonLeaf : "when splitting a non leaf node, needs same type";

        final BTreeNodeNonLeaf bTreeNodeLeaf = (BTreeNodeNonLeaf) splitNode;
        if (getNumberOfLogPairs() > 0)
        {
            final byte[] keyAt = getKey(at);
            splitLog(keyAt, bTreeNodeLeaf);
        }

        final int aNumberOfPairs = at + 1;
        final int bNumberOfPairs = entries.getNumberOfPairs() - aNumberOfPairs;

        final BTreeNodeHeap[] aChildren = new BTreeNodeHeap[aNumberOfPairs];
        final BTreeNodeHeap[] bChildren = new BTreeNodeHeap[bNumberOfPairs];
        System.arraycopy(children, 0, aChildren, 0, aNumberOfPairs);
        System.arraycopy(children, aNumberOfPairs, bChildren, 0, bNumberOfPairs);
        children = aChildren;

        bTreeNodeLeaf.setChildren(bChildren);

        //set bnode rightmost value
        final int rightmostIndex = (aNumberOfPairs + bNumberOfPairs) - 1;
        final byte[] rightmostValue = entries.getValueAtIndex(rightmostIndex);
        bTreeNodeLeaf.setValue(0, rightmostValue);
        removeKeyAndValueWithCell(rightmostIndex, getPairCount());

        //move pairs from this to bnode
        for (int i = 0; i < bNumberOfPairs - 1; i++)
        {
            final byte[] key = entries.getKeyAtIndex(aNumberOfPairs);
            final byte[] value = entries.getValueAtIndex(aNumberOfPairs);
            bTreeNodeLeaf.insert(key, value);
            removeKeyAndValueWithCell(aNumberOfPairs, getPairCount());
        }

        final int mostRightPairIndex = aNumberOfPairs - 1;
        entries.removeOnlyKey(mostRightPairIndex);

        bTreeNodeLeaf.setDirty();
        setDirty();
    }

    void setChildren(final BTreeNodeHeap[] children)
    {
        this.children = children;
    }

    @Override
    public @PageNumber long commit(final NodesManager nodesManager,
                       final boolean isRoot,
                       final @PageNumber long previousRootPageNumber,
                       final @Milliseconds long timestamp,
                       final @Version long version) throws IOException
    {
        if (isDirty)
        {
            for (int index = 0; index < children.length; index++)
            {
                final BTreeNode child = children[index];
                if (child != null)
                {
                    final long pageNumber;
                    if (child.isDirty())
                    {
                        pageNumber = child.commit(nodesManager, false, previousRootPageNumber, timestamp, version);
                    }
                    else
                    {
                        pageNumber = child.getPageNumber();
                    }

                    setValue(index, pageNumber);
                    children[index] = null;
                }
            }

            preCommit(isRoot, previousRootPageNumber, timestamp, version);
            this.pageNumber = nodesManager.commitNode(this);
        }

        return this.pageNumber;
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.NonLeaf;
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param gapIndex the index of the gap
     */
    private static void copyWithGap(
            final Object[] src,
            final Object[] dst,
            final int oldSize,
            final int gapIndex)
    {
        if (gapIndex > 0)
        {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize)
        {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize - gapIndex);
        }
    }

    /**
     * Copy the elements of an array, and remove one element.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param removeIndex the index of the entry to remove
     */
    private static void copyExcept(
            final Object[] src,
            final Object[] dst,
            final int oldSize,
            final int removeIndex)
    {
        if (removeIndex > 0 && oldSize > 0)
        {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize)
        {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize - removeIndex - 1);
        }
    }

    @Override
    public HeapMemory getBuffer()
    {
        return (HeapMemory)buffer;
    }
}
