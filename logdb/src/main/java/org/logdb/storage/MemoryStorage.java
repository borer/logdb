package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final @ByteSize int pageSizeBytes;
    private final HashMap<Long, DirectMemory> maps;

    private @ByteOffset long allocatedMemoryOffset;
    private @ByteOffset long lastPersistedOffset;

    public MemoryStorage(final ByteOrder byteOrder, final @ByteSize int pageSizeBytes)
    {
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.maps = new HashMap<>();
        this.allocatedMemoryOffset = ZERO_OFFSET;
        this.lastPersistedOffset = StorageUnits.INVALID_OFFSET;
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
    public ByteOrder getOrder()
    {
        return byteOrder;
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
    public @ByteOffset long append(final ByteBuffer buffer)
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
        return StorageUnits.pageNumber(append(buffer));
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
    public @ByteOffset long getLastPersistedOffset()
    {
        return lastPersistedOffset;
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, @Version long version)
    {
        this.lastPersistedOffset = lastPersistedOffset;
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
