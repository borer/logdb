package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final int pageSizeBytes;
    private final HashMap<Long, DirectMemory> maps;

    private long allocatedMemoryOffset;
    private @PageNumber long lastPageRootNumber;

    public MemoryStorage(final ByteOrder byteOrder,
                         final int pageSizeBytes)
    {
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.maps = new HashMap<>();
        this.allocatedMemoryOffset = 0L;
        this.lastPageRootNumber = StorageUnits.pageNumber(-1L);
    }

    @Override
    public HeapMemory allocateHeapPage()
    {
        return MemoryFactory.allocateHeap(pageSizeBytes, byteOrder);
    }

    @Override
    public DirectMemory getUninitiatedDirectMemoryPage()
    {
        return MemoryFactory.getUninitiatedDirectMemory(pageSizeBytes, byteOrder);
    }

    @Override
    public long getPageSize()
    {
        return pageSizeBytes;
    }

    @Override
    public @PageNumber long getPageNumber(final long offset)
    {
        return StorageUnits.pageNumber(offset);
    }

    @Override
    public long getOffset(final @PageNumber long pageNumber)
    {
        return pageNumber;
    }

    @Override
    public long write(final ByteBuffer buffer)
    {
        final long currentOffset = this.allocatedMemoryOffset;
        allocatedMemoryOffset += buffer.capacity();

        final DirectMemory directMemory = MemoryFactory.allocateDirect(pageSizeBytes, byteOrder);
        directMemory.putBytes(buffer.array());

        maps.put(currentOffset, directMemory);

        return currentOffset;
    }

    @Override
    public @PageNumber long writePageAligned(final ByteBuffer buffer)
    {
        return StorageUnits.pageNumber(write(buffer));
    }

    @Override
    public void flush()
    {
        //NO-OP
    }

    @Override
    public void close()
    {
        //NO-OP
    }

    @Override
    public @PageNumber long getLastRootPageNumber()
    {
        return lastPageRootNumber;
    }

    @Override
    public void commitMetadata(final @PageNumber long lastRootPageNumber, @Version long version)
    {
        this.lastPageRootNumber = lastRootPageNumber;
    }

    @Override
    public DirectMemory loadPage(final @PageNumber long pageNumber)
    {
        return maps.get(pageNumber);
    }

    @Override
    public long getBaseOffsetForPageNumber(final @PageNumber long pageNumber)
    {
        final DirectMemory directMemory = maps.get(pageNumber);
        if (directMemory == null)
        {
            return pageNumber;
        }

        return directMemory.getBaseAddress();
    }
}
