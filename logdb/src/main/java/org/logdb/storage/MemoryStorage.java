package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryFactory;
import org.logdb.bit.ReadMemory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final int pageSizeBytes;
    private final HashMap<Long, ReadMemory> maps;

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
    public Memory allocateHeapMemory()
    {
        return MemoryFactory.allocateHeap(pageSizeBytes, byteOrder);
    }

    @Override
    public void returnWritableMemory(final Memory writableMemory)
    {
        //NO-OP
    }

    @Override
    public DirectMemory getDirectMemory(final long pageNumber)
    {
        return new DirectMemory()
        {
            @Override
            public void setBaseAddress(long baseAddress)
            {
                //No-op
            }

            @Override
            public Memory toMemory()
            {
                final Memory memory = (Memory) maps.get(pageNumber);
                if (memory == null)
                {
                    return allocateHeapMemory();
                }
                return memory;
            }
        };
    }

    @Override
    public long getPageSize()
    {
        return pageSizeBytes;
    }

    @Override
    public long commitNode(final ReadMemory node)
    {
        final long currentOffset = this.allocatedMemoryOffset;
        allocatedMemoryOffset += node.getCapacity();

        maps.put(currentOffset, node);

        return currentOffset;
    }

    @Override
    public void flush()
    {
        //NO-OP
    }

    @Override
    public void close() throws IOException
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
    public Memory loadPage(final long pageNumber)
    {
        return (Memory)maps.get(pageNumber);
    }

    @Override
    public long getBaseOffsetForPageNumber(long pageNumber)
    {
        return pageNumber;
    }
}
