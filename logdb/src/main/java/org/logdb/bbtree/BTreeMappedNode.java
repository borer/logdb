package org.logdb.bbtree;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;

import java.util.function.Consumer;

public class BTreeMappedNode extends BTreeLogNodeAbstract implements AutoCloseable
{
    private final Consumer<BTreeMappedNode> closeHandler;
    private final Storage storage;
    private final @ByteSize int maxLogSize;

    public BTreeMappedNode(
            final Consumer<BTreeMappedNode> closeHandler,
            final Storage storage,
            final DirectMemory memory,
            final @PageNumber long pageNumber,
            final @ByteSize int maxLogSize)
    {
        super(pageNumber, memory, maxLogSize, 0);
        this.closeHandler = closeHandler;
        this.storage = storage;
        this.maxLogSize = maxLogSize;
    }

    /**
     * After setting the page number and pagen number offset, read memory to initialize the node.
     * @param pageNumber the page number
     */
    public void initNode(final @PageNumber long pageNumber)
    {
        assert buffer instanceof DirectMemory;

        this.pageNumber = pageNumber;
        storage.mapPage(pageNumber, (DirectMemory) buffer);

        final Memory logMemory = logHeap.getMemory();
        if (logMemory instanceof DirectMemory)
        {
            final @ByteOffset short logStartOffset = StorageUnits.offset((short)getLogStartOffset(buffer, maxLogSize));
            final @ByteOffset long newBaseAddress = StorageUnits.offset(buffer.getBaseAddress() + logStartOffset);
            final DirectMemory directMemory = (DirectMemory) logMemory;
            directMemory.setBaseAddress(newBaseAddress);
        }

        final Memory entriesMemory = entries.getMemory();
        if (entriesMemory instanceof DirectMemory)
        {
            final DirectMemory directMemory = (DirectMemory) entriesMemory;
            final @ByteOffset long baseAddress = StorageUnits.offset(buffer.getBaseAddress() + BTreeNodePage.PAGE_HEADER_SIZE);
            directMemory.setBaseAddress(baseAddress);
        }

        initNodeFromBuffer();
    }

    private void initNodeFromBuffer()
    {
        reloadCacheValuesFromBuffer();
        refreshKeyValueLog();
    }

    @Override
    public BtreeNodeType getNodeType()
    {
        return BtreeNodeType.fromByte(buffer.getByte(BTreeNodePage.PAGE_TYPE_OFFSET));
    }

    @Override
    public void insert(final byte[] key, final byte[] value)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support insertion");
    }

    @Override
    public void removeAtIndex(final int index)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support removal");
    }

    @Override
    public byte[] get(final byte[] key)
    {
        int index = getKeyIndex(key);
        if (getNodeType() == BtreeNodeType.Leaf)
        {
            if (index < 0)
            {
                return null;
            }
        }
        return getValue(index);
    }

    @Override
    public int getKeyIndex(final byte[] key)
    {
        final boolean isNonLeaf = getNodeType() == BtreeNodeType.NonLeaf;
        int index = isNonLeaf ? binarySearchNonLeaf(key) + 1 : binarySearch(key);
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
        //TODO: make the representation of BTreeNodeHeap children be a bit more memory friendly
        final HeapMemory destinationNodeBuffer = destinationNode.getBuffer();
        MemoryCopy.copy(this.buffer, destinationNodeBuffer, destinationNodeBuffer.getCapacity());
        destinationNode.initNodeFromBuffer();

        if (destinationNode.getNodeType() == BtreeNodeType.NonLeaf && (destinationNode instanceof BTreeNodeNonLeaf))
        {
            ((BTreeNodeNonLeaf) destinationNode).setChildren(new BTreeNodeHeap[entries.getNumberOfPairs()]);
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
    public void insertChild(final int index, final byte[] key, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support insertion of children");
    }

    @Override
    public void setChild(final int index, final BTreeNodeHeap child)
    {
        throw new UnsupportedOperationException("Mapped node doesn't support setting children");
    }

    @Override
    public void close()
    {
        closeHandler.accept(this);
    }
}
