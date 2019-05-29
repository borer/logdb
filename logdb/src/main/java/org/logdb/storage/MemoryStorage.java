package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.bit.MemoryFactory;
import org.logdb.bit.ReadMemory;

import java.nio.ByteOrder;
import java.util.HashMap;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final int pageSizeBytes;
    private final HashMap<Long, DirectMemory> maps;

    private long allocatedMemoryOffset;
    private long lastPageRootNumber;

    public MemoryStorage(final ByteOrder byteOrder,
                         final int pageSizeBytes)
    {
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.maps = new HashMap<>();
        this.allocatedMemoryOffset = 0L;
        this.lastPageRootNumber = -1L;
    }

    @Override
    public HeapMemory allocateHeapMemory()
    {
        return MemoryFactory.allocateHeap(pageSizeBytes, byteOrder);
    }

    @Override
    public DirectMemory getDirectMemory(final long pageNumber)
    {
        final DirectMemory directMemory = maps.get(pageNumber);
        if (directMemory == null)
        {
            return MemoryFactory.allocateDirect(pageSizeBytes, byteOrder);
        }
        return directMemory;
    }

    @Override
    public long getPageSize()
    {
        return pageSizeBytes;
    }

    @Override
    public long writeNode(final ReadMemory node)
    {
        final long currentOffset = this.allocatedMemoryOffset;
        allocatedMemoryOffset += node.getCapacity();

        final DirectMemory directMemory = MemoryFactory.allocateDirect(pageSizeBytes, byteOrder);
        MemoryCopy.copy((Memory)node, directMemory);

        maps.put(currentOffset, directMemory);

        return currentOffset;
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
    public long getLastRootPageNumber()
    {
        return lastPageRootNumber;
    }

    @Override
    public void commitMetadata(final long lastRootPageNumber)
    {
        this.lastPageRootNumber = lastRootPageNumber;
    }

    @Override
    public DirectMemory loadPage(final long pageNumber)
    {
        return maps.get(pageNumber);
    }

    @Override
    public long getBaseOffsetForPageNumber(long pageNumber)
    {
        final DirectMemory directMemory = maps.get(pageNumber);
        if (directMemory == null)
        {
            return pageNumber;
        }

        return directMemory.getBaseAddress();
    }
}
