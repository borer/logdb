package org.logdb.storage.memory;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.bit.MemoryOrder;
import org.logdb.bit.NativeMemoryAccess;
import org.logdb.bit.NonNativeMemoryAccess;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.logdb.storage.StorageUnits.INITIAL_VERSION;
import static org.logdb.storage.StorageUnits.INVALID_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryStorage implements Storage
{
    private final ByteOrder order;
    private final @ByteSize int pageSize;
    private final List<DirectMemory> bufferPool;
    private final @ByteSize int memoryChunkSize;

    private @ByteOffset long allocatedMemoryOffset;

    private @ByteOffset long currentMemoryChunkOffset;

    private @ByteOffset long lastPersistedOffset;
    private @Version long version;

    public MemoryStorage(
            final ByteOrder order,
            final @ByteSize int pageSize,
            final @ByteSize int memoryChunkSize)
    {
        this.order = order;
        this.pageSize = pageSize;
        this.memoryChunkSize = memoryChunkSize;
        this.bufferPool = new ArrayList<>();

        final DirectMemory directMemory = MemoryFactory.allocateDirect(memoryChunkSize, order);
        bufferPool.add(directMemory);

        this.allocatedMemoryOffset = ZERO_OFFSET;
        this.currentMemoryChunkOffset = ZERO_OFFSET;

        this.lastPersistedOffset = StorageUnits.INVALID_OFFSET;
        this.version = INITIAL_VERSION;
    }

    @Override
    public HeapMemory allocateHeapPage()
    {
        return MemoryFactory.allocateHeap(pageSize, order);
    }

    @Override
    public DirectMemory getUninitiatedDirectMemoryPage()
    {
        return MemoryFactory.getUninitiatedDirectMemory(pageSize, order);
    }

    @Override
    public @ByteSize long getPageSize()
    {
        return pageSize;
    }

    @Override
    public ByteOrder getOrder()
    {
        return order;
    }

    @Override
    public @PageNumber long getPageNumber(final @ByteOffset long offset)
    {
        return StorageUnits.pageNumber(offset / pageSize);
    }

    @Override
    public @ByteOffset long getOffset(final @PageNumber long pageNumber)
    {
        return StorageUnits.offset(pageNumber * pageSize);
    }

    @Override
    public @ByteOffset long append(final ByteBuffer buffer)
    {
        final @ByteOffset int bufferSize = StorageUnits.offset(buffer.capacity());
        final @ByteOffset long expectedOffset = currentMemoryChunkOffset + bufferSize;

        final @ByteOffset long currentOffset;
        if (expectedOffset > memoryChunkSize)
        {
            final @ByteOffset long remaining = StorageUnits.offset(memoryChunkSize - currentMemoryChunkOffset);
            allocatedMemoryOffset += remaining;

            final DirectMemory directMemory = MemoryFactory.allocateDirect(memoryChunkSize, order);
            bufferPool.add(directMemory);

            currentMemoryChunkOffset = ZERO_OFFSET;
        }

        final @ByteOffset long baseOffset = getBaseOffset(allocatedMemoryOffset);
        if (MemoryOrder.isNativeOrder(order))
        {
            NativeMemoryAccess.putBytes(baseOffset, buffer.array());
        }
        else
        {
            NonNativeMemoryAccess.putBytes(baseOffset, buffer.array());
        }

        currentOffset = this.allocatedMemoryOffset;
        allocatedMemoryOffset += bufferSize;
        currentMemoryChunkOffset += bufferSize;

        return currentOffset;
    }

    @Override
    public @PageNumber long appendPageAligned(final ByteBuffer buffer)
    {
        return getPageNumber(append(buffer));
    }

    @Override
    public void flush(final boolean flushMeta)
    {
        //NO-OP
    }

    @Override
    public void close()
    {
        //NO-OP
    }

    @Override
    public @ByteOffset long getLastPersistedOffset()
    {
        return lastPersistedOffset;
    }

    @Override
    public @PageNumber long getLastPersistedPageNumber()
    {
        return getPageNumber(lastPersistedOffset);
    }

    @Override
    public @Version long getAppendVersion()
    {
        return version;
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, final @Version long version)
    {
        this.lastPersistedOffset = lastPersistedOffset;
        this.version = version;
    }

    @Override
    public void mapPage(final @PageNumber long pageNumber, final DirectMemory memory)
    {
        final @ByteOffset long pageOffset = getOffset(pageNumber);
        final @ByteOffset long baseOffset = getBaseOffset(pageOffset);
        memory.setBaseAddress(baseOffset);
    }

    private @ByteOffset long getBaseOffset(final @ByteOffset long pageOffset)
    {
        assert pageOffset >= 0 : "Offset can only be positive. Provided " + pageOffset;
        assert pageOffset <= StorageUnits.offset(bufferPool.size() * memoryChunkSize)
                : "The offset " + pageOffset + " is outside the mapped range of " +
                (StorageUnits.offset(bufferPool.size() * memoryChunkSize));

        @ByteOffset long offsetBuffer = StorageUnits.ZERO_OFFSET;

        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        for (int i = 0; i < bufferPool.size(); i++)
        {
            final DirectMemory buffer = bufferPool.get(i);
            final @ByteOffset long bufferEndOffset = StorageUnits.offset(offsetBuffer + buffer.getCapacity());
            if (pageOffset >= offsetBuffer && pageOffset < bufferEndOffset)
            {
                final @ByteOffset long offsetInsideBuffer = StorageUnits.offset(pageOffset - offsetBuffer);
                return StorageUnits.offset(buffer.getBaseAddress() + offsetInsideBuffer);
            }

            offsetBuffer += StorageUnits.offset(buffer.getCapacity());
        }

        return INVALID_OFFSET;
    }

    @Override
    public void readBytes(final @ByteOffset long offset, final ByteBuffer buffer)
    {
        final @ByteSize int lengthBytes = StorageUnits.size(buffer.capacity());
        @ByteOffset int readPosition = ZERO_OFFSET;

        @PageNumber long pageNumber = getPageNumber(offset);
        final @ByteOffset long pageNumberOffset = getOffset(pageNumber);
        @ByteOffset long offsetInsidePage = offset - pageNumberOffset;

        final @ByteOffset long baseOffset = getBaseOffset(pageNumberOffset);

        @ByteSize long pageLeftSpace = StorageUnits.size(pageSize - offsetInsidePage);
        final @ByteSize long bytesToRead = StorageUnits.size(
                Math.min(pageLeftSpace, lengthBytes));

        readPosition += StorageUnits.offset(bytesToRead);

        if (MemoryOrder.isNativeOrder(order))
        {
            NativeMemoryAccess.getBytes(
                    baseOffset + offsetInsidePage,
                    buffer.array(),
                    ZERO_OFFSET,
                    lengthBytes);
        }
        else
        {
            NonNativeMemoryAccess.getBytes(
                    baseOffset + offsetInsidePage,
                    buffer.array(),
                    ZERO_OFFSET,
                    lengthBytes);
        }

        while (readPosition < lengthBytes)
        {
            //continue reading the header from the beginning of the next page
            pageNumber++;

            final @ByteOffset long baseOffset2 = getBaseOffset(getOffset(pageNumber));
            final @ByteSize long bytesToRead2 = StorageUnits.size(Math.min(pageSize, lengthBytes));

            if (MemoryOrder.isNativeOrder(order))
            {
                NativeMemoryAccess.getBytes(
                        baseOffset2,
                        buffer.array(),
                        readPosition,
                        bytesToRead2);
            }
            else
            {
                NonNativeMemoryAccess.getBytes(
                        baseOffset2,
                        buffer.array(),
                        readPosition,
                        bytesToRead2);
            }

            readPosition += StorageUnits.offset(bytesToRead2);
        }
    }
}
