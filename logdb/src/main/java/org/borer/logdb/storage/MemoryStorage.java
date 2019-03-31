package org.borer.logdb.storage;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;

import java.nio.ByteOrder;
import java.util.HashMap;

public class MemoryStorage implements Storage
{
    private final ByteOrder byteOrder;
    private final int pageSizeBytes;
    private final HashMap<Long, Memory> maps;

    private long allocatedMemoryOffset;
    private long lastPageRootNumber;

    public MemoryStorage(final ByteOrder byteOrder,
                         final int pageSizeBytes)
    {
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.maps = new HashMap<>();
        this.allocatedMemoryOffset = 0L;
        this.lastPageRootNumber = 0L;
    }

    @Override
    public Memory allocateWritableMemory()
    {
        return MemoryFactory.allocateHeap(pageSizeBytes, byteOrder);
    }

    @Override
    public void returnWritableMemory(final Memory writableMemory)
    {
        //NO-OP
    }

    @Override
    public long commitNode(final Memory node)
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
    public Memory loadLastRoot()
    {
        return loadPage(lastPageRootNumber);
    }

    @Override
    public Memory loadPage(final long pageOffset)
    {
        return maps.get(pageOffset);
    }
}
