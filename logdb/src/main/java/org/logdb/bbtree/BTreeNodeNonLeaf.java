package org.logdb.bbtree;

import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.io.IOException;

public class BTreeNodeNonLeaf extends BTreeLogNodeAbstract implements BTreeNodeHeap
{
    static final int NON_COMMITTED_CHILD = Integer.MIN_VALUE;

    private BTreeNodeHeap[] children;

    public BTreeNodeNonLeaf(
            final @PageNumber long pageNumber,
            final HeapMemory memory,
            final @ByteSize int maxLogSize,
            final int numberOfLogKeyValues,
            final int numberOfKeys,
            final int numberOfValues,
            final BTreeNodeHeap[] children)
    {
        super(pageNumber, memory, maxLogSize, numberOfLogKeyValues, numberOfKeys, numberOfValues);
        this.children = children;
    }

    public static BTreeNodeNonLeaf load(final @PageNumber long pageNumber, final HeapMemory memory, final @ByteSize int maxLogSize)
    {
        return new BTreeNodeNonLeaf(
                pageNumber,
                memory,
                maxLogSize,
                memory.getInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET),
                memory.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET),
                memory.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET),
                new BTreeNodeHeap[0]);
    }

    @Override
    public void insert(final long key, final long value)
    {
        throw new RuntimeException(
                String.format("Cannot insert elements in non leaf node. Key to insert %d, value %d", key, value));
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
    public void insertChild(final int index, final long key, final BTreeNodeHeap child)
    {
        final int rawChildPageCount = getNumberOfChildren();
        //this will be replaced once we commit the child page
        insertKeyAndValue(index, key, NON_COMMITTED_CHILD);

        BTreeNodeHeap[] newChildren = new BTreeNodeHeap[rawChildPageCount + 1];
        copyWithGap(children, newChildren, rawChildPageCount, index);
        children = newChildren;
        children[index] = child;

        setDirty();
    }

    @Override
    public int getNumberOfChildren()
    {
        return getKeyCount() + 1;
    }

    @Override
    public void remove(final int index)
    {
        final int keyCount = getKeyCount();

        assert keyCount >= index && keyCount > 0
                : String.format("removing index %d when key count is %d", index, keyCount);

        removeKeyAndValue(index, keyCount);

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
    public long get(final long key)
    {
        int index = getKeyIndex(key);
        return getValue(index);
    }

    @Override
    public int getKeyIndex(final long key)
    {
        int index = binarySearch(key) + 1;
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
        numberOfKeys = buffer.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET);
        numberOfValues = buffer.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET);
        numberOfLogKeyValues = buffer.getInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET);

        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        assert getKeyCount() > 0 : "cannot split node with less than 2 nodes";
        assert splitNode instanceof BTreeNodeNonLeaf : "when splitting a non leaf node, needs same type";

        final int aNumberOfValues = at + 1;
        final int bNumberOfKeys = numberOfKeys - aNumberOfValues;
        final int bNumberOfValues = numberOfValues - aNumberOfValues;

        final BTreeNodeHeap[] aChildren = new BTreeNodeHeap[aNumberOfValues];
        final BTreeNodeHeap[] bChildren = new BTreeNodeHeap[bNumberOfValues];
        System.arraycopy(children, 0, aChildren, 0, aNumberOfValues);
        System.arraycopy(children, aNumberOfValues, bChildren, 0, bNumberOfValues);
        children = aChildren;

        final BTreeNodeNonLeaf bTreeNodeLeaf = (BTreeNodeNonLeaf) splitNode;
        bTreeNodeLeaf.setChildren(bChildren);
        bTreeNodeLeaf.updateNumberOfKeys(bNumberOfKeys);
        bTreeNodeLeaf.updateNumberOfValues(bNumberOfValues);

        splitKeysAndValues(at, bNumberOfKeys, bTreeNodeLeaf);

        if (numberOfLogKeyValues > 0)
        {
            final long keyAt = getKey(at);
            splitLog(keyAt, bTreeNodeLeaf);
        }

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
