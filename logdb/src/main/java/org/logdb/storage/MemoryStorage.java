package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

import static org.logdb.storage.StorageUnits.INVALID_PAGE_NUMBER;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final @ByteSize int pageSizeBytes;
    private final HashMap<Long, DirectMemory> maps;

    private @ByteOffset long allocatedMemoryOffset;
    private @PageNumber long lastPageRootNumber;

    public MemoryStorage(final ByteOrder byteOrder, final @ByteSize int pageSizeBytes)
    {
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.maps = new HashMap<>();
        this.allocatedMemoryOffset = ZERO_OFFSET;
        this.lastPageRootNumber = INVALID_PAGE_NUMBER;
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
    public @ByteSize long getPageSize()
    {
        return pageSizeBytes;
    }

    @Override
    public @PageNumber long getPageNumber(final @ByteOffset long offset)
    {
        return StorageUnits.pageNumber(offset);
    }

    @Override
    public @ByteOffset long getOffset(final @PageNumber long pageNumber)
    {
        return StorageUnits.offset(pageNumber);
    }

    @Override
    public @ByteOffset long write(final ByteBuffer buffer)
    {
        final @ByteOffset long currentOffset = this.allocatedMemoryOffset;
        allocatedMemoryOffset += StorageUnits.offset(buffer.capacity());

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
    public @ByteOffset long getBaseOffsetForPageNumber(final @PageNumber long pageNumber)
    {
        final DirectMemory directMemory = maps.get(pageNumber);
        if (directMemory == null)
        {
            return StorageUnits.offset(pageNumber);
        }

        return directMemory.getBaseAddress();
    }
}
