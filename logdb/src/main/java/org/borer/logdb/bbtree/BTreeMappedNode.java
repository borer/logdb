package org.borer.logdb.bbtree;

import org.borer.logdb.bit.DirectMemory;
import org.borer.logdb.bit.MemoryCopy;
import org.borer.logdb.storage.NodesManager;
import org.borer.logdb.storage.Storage;

import static org.borer.logdb.bbtree.BTreeNodePage.PAGE_TYPE_OFFSET;

public class BTreeMappedNode extends BTreeNodeAbstract
{
    private final Storage storage;
    private final DirectMemory memory;

    private long baseOffset;

    public BTreeMappedNode(
            final Storage storage,
            final DirectMemory memory,
            final long pageSize,
            final long pageNumber)
    {
        super(pageNumber, memory.toMemory(), 0, 0);
        this.storage = storage;
        this.memory = memory;

        this.baseOffset = pageNumber * pageSize;
    }

    /**
     * After setting the page number and pagen number offset, read memory to initialize the node.
     * @param pageNumber the page number
     */
    public void initNode(final long pageNumber)
    {
        this.pageNumber = pageNumber;
        this.baseOffset = storage.getBaseOffsetForPageNumber(pageNumber);
        memory.setBaseAddress(baseOffset);
        super.initNodeFromBuffer();
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.fromByte(buffer.getByte(PAGE_TYPE_OFFSET));
    }

    @Override
    public void insert(long key, long value)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support insertion");
    }

    @Override
    public void remove(int index)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support removal");
    }

    @Override
    public long get(long key)
    {
        int index = getKeyIndex(key);
        if (getNodeType() == BtreeNodeType.Leaf)
        {
            index--;
        }
        return getValue(index);
    }

    @Override
    public int getKeyIndex(long key)
    {
        int index = binarySearch(key) + 1;
        if (index < 0)
        {
            index = -index;
        }

        return index;
    }

    @Override
    public void split(final int at, final BTreeNodeHeap splitNode)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support split");
    }

    @Override
    public void copy(final BTreeNodeHeap copyNode)
    {
        MemoryCopy.copy(buffer, copyNode.getBuffer());
        copyNode.initNodeFromBuffer();

        if (copyNode.getNodeType() == BtreeNodeType.NonLeaf && (copyNode instanceof BTreeNodeNonLeaf))
        {
            ((BTreeNodeNonLeaf)copyNode).setChildren(new BTreeNodeHeap[numberOfValues]);
        }
    }

    @Override
    public long commit(final NodesManager nodesManager,
                       final boolean isRoot,
                       final long previousRootPageNumber,
                       final long timestamp,
                       final long version)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support commit");
    }

    @Override
    public BTreeNode getChildAt(int index)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support getting children at position");
    }

    @Override
    public void insertChild(final int index, final long key, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support insertion of children");
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support setting children");
    }

    @Override
    public int getChildrenNumber()
    {
        return numberOfValues;
    }
}
