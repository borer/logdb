package org.logdb.bbtree;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.util.function.Consumer;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class BTreeMappedNode extends BTreeLogNodeAbstract implements AutoCloseable
{
    private final Consumer<BTreeMappedNode> closeHandler;
    private final Storage storage;
    private final DirectMemory memory;

    public BTreeMappedNode(
            final Consumer<BTreeMappedNode> closeHandler,
            final Storage storage,
            final DirectMemory memory,
            final @PageNumber long pageNumber,
            final @ByteSize int maxLogSize)
    {
        super(pageNumber, memory, maxLogSize, 0, 0, 0);
        this.closeHandler = closeHandler;
        this.storage = storage;
        this.memory = memory;
    }

    /**
     * After setting the page number and pagen number offset, read memory to initialize the node.
     * @param pageNumber the page number
     */
    public void initNode(final @PageNumber long pageNumber)
    {
        this.pageNumber = pageNumber;
        storage.mapPage(pageNumber, memory);
        initNodeFromBuffer();
    }

    private void initNodeFromBuffer()
    {
        numberOfKeys = buffer.getInt(BTreeNodePage.NUMBER_OF_KEY_OFFSET);
        numberOfValues = buffer.getInt(BTreeNodePage.NUMBER_OF_VALUES_OFFSET);

        if (getNodeType() == BtreeNodeType.NonLeaf)
        {
            numberOfLogKeyValues = buffer.getInt(BTreeNodePage.PAGE_LOG_KEY_VALUE_NUMBERS_OFFSET);
        }
        else
        {
            numberOfLogKeyValues = 0;
        }

        freeSizeLeftBytes = calculateFreeSpaceLeft(buffer.getCapacity());
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.fromByte(buffer.getByte(BTreeNodePage.PAGE_TYPE_OFFSET));
    }

    @Override
    public void insert(final long key, final long value)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support insertion");
    }

    @Override
    public void remove(final int index)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support removal");
    }

    @Override
    public long get(final long key)
    {
        int index = getKeyIndex(key);
        if (getNodeType() == BtreeNodeType.Leaf)
        {
            if (index < 0)
            {
                return KEY_NOT_FOUND_VALUE;
            }
        }
        return getValue(index);
    }

    @Override
    public int getKeyIndex(final long key)
    {
        final boolean isNonLeaf = getNodeType() == BtreeNodeType.NonLeaf;
        final int nonLeafAddition = isNonLeaf ? 1 : 0;
        int index = binarySearch(key) + nonLeafAddition;
        if (index < 0 && isNonLeaf)
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
    public void copy(final BTreeNodeHeap destinationNode)
    {
        final HeapMemory destinationNodeBuffer = destinationNode.getBuffer();
        MemoryCopy.copy(this.buffer, destinationNodeBuffer, destinationNodeBuffer.getCapacity());
        destinationNode.initNodeFromBuffer();

        if (destinationNode.getNodeType() == BtreeNodeType.NonLeaf && (destinationNode instanceof BTreeNodeNonLeaf))
        {
            ((BTreeNodeNonLeaf) destinationNode).setChildren(new BTreeNodeHeap[numberOfValues]);
        }
    }

    @Override
    public @PageNumber long commit(final NodesManager nodesManager,
                       final boolean isRoot,
                       final @PageNumber long previousRootPageNumber,
                       final @Milliseconds long timestamp,
                       final @Version long version)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support commit");
    }

    @Override
    public BTreeNode getChildAt(final int index)
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
    public int getNumberOfChildren()
    {
        return numberOfValues;
    }

    @Override
    public void close()
    {
        closeHandler.accept(this);
    }
}
