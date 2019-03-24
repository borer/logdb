package org.borer.logdb.storage;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;

import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class MemoryStorage implements Storage
{
    private final long memoryMappedChunkSizeBytes;
    private final ByteOrder byteOrder;
    private final List<MappedByteBuffer> mappedBuffers;
    private final Queue<Memory> availableWritableMemory;

    private int pageSizeBytes;

    public MemoryStorage(final long memoryMappedChunkSizeBytes,
                         final ByteOrder byteOrder,
                         final int pageSizeBytes)
    {
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        this.byteOrder = byteOrder;
        this.pageSizeBytes = pageSizeBytes;
        this.mappedBuffers = new ArrayList<>();
        this.availableWritableMemory = new ArrayDeque<>();
    }

    @Override
    public Memory allocateWritableMemory()
    {
        final Memory writableMemory = availableWritableMemory.poll();
        if (writableMemory != null)
        {
            return writableMemory;
        }
        else
        {
            return MemoryFactory.allocateHeap(pageSizeBytes, byteOrder);
        }
    }

    @Override
    public void returnWritableMemory(final Memory writableMemory)
    {
        availableWritableMemory.add(writableMemory);
    }

    @Override
    public long commitNode(final Memory node)
    {
        //NO-OP
        return -1;
    }

    @Override
    public void flush()
    {
        //NO-OP
    }

    @Override
    public void commitMetadata(final long lastRootPageNumber)
    {
        //NO-OP
    }

    @Override
    public Memory loadLastRoot()
    {
        return null;
    }
}
