package org.borer.logdb.storage;

import org.borer.logdb.Config;
import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class FileStorage
{
    private final String filename;
    private final long memoryMappedChunkSizeBytes;
    private final ByteOrder byteOrder;
    private final List<MappedByteBuffer> mappedBuffers;
    private final Queue<Memory> availableWritableMemory;

    private RandomAccessFile dbFile;
    private FileChannel channel;

    public FileStorage(final String filename,
                       final long memoryMappedChunkSizeBytes,
                       final ByteOrder byteOrder)
    {
        this.filename = filename;
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        this.byteOrder = byteOrder;
        this.mappedBuffers = new ArrayList<>();
        this.availableWritableMemory = new ArrayDeque<>();
    }

    private void init()
    {
        try
        {
            dbFile = new RandomAccessFile(filename, "rw");
            channel = dbFile.getChannel();
            final MappedByteBuffer map = channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, memoryMappedChunkSizeBytes);

            //try to get file size down after the mapping
            channel.truncate(mappedBuffers.size() * memoryMappedChunkSizeBytes);

            mappedBuffers.add(map);
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public Memory allocateWritableMemory()
    {
        final Memory writableMemory = availableWritableMemory.poll();
        if (writableMemory != null)
        {
            return writableMemory;
        }
        else
        {
            return MemoryFactory.allocateHeap(Config.PAGE_SIZE_BYTES, byteOrder);
        }
    }

    public void returnWritableMemory(final Memory writableMemory)
    {
        availableWritableMemory.add(writableMemory);
    }
}
