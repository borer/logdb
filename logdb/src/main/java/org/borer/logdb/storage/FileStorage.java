package org.borer.logdb.storage;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.borer.logdb.Config.LOG_DB_VERSION;

public final class FileStorage implements Storage, Closeable
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int NO_CURRENT_ROOT_OFFSET = -1;

    private final String filename;
    private final List<MappedByteBuffer> mappedBuffers;
    private final Queue<Memory> availableWritableMemory;

    private final RandomAccessFile dbFile;
    private final FileChannel channel;
    private final FileDbHeader fileDbHeader;

    private FileStorage(
            final String filename,
            final FileDbHeader fileDbHeader,
            final RandomAccessFile dbFile,
            final FileChannel channel)
    {
        this.filename = filename;
        this.fileDbHeader = fileDbHeader;
        this.dbFile = dbFile;
        this.channel = channel;
        this.mappedBuffers = new ArrayList<>();
        this.availableWritableMemory = new ArrayDeque<>();
    }

    public static FileStorage createNewFileDb(
            final String filename,
            final long memoryMappedChunkSizeBytes,
            final ByteOrder byteOrder,
            final int pageSizeBytes)
    {
        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(filename, "rw");
            final FileChannel channel = dbFile.getChannel();

            final ByteBuffer headerBuffer = ByteBuffer.allocate(FileDbHeader.getSizeBytes());
            final FileDbHeader fileDbHeader = new FileDbHeader(
                    byteOrder,
                    LOG_DB_VERSION,
                    pageSizeBytes,
                    memoryMappedChunkSizeBytes,
                    NO_CURRENT_ROOT_OFFSET
            );

            headerBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
            fileDbHeader.writeTo(headerBuffer);
            headerBuffer.rewind();

            channel.write(headerBuffer);
            channel.position(FileDbHeader.getSizeBytes());

            channel.force(true);

            fileStorage = new FileStorage(
                    filename,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initMaps();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return fileStorage;
    }

    public static FileStorage openDbFile(final String filename)
    {
        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(filename, "rw");
            final FileChannel channel = dbFile.getChannel();

            final ByteBuffer headerBuffer = ByteBuffer.allocate(FileDbHeader.getSizeBytes());
            headerBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
            channel.read(headerBuffer);
            channel.position(FileDbHeader.getSizeBytes());
            headerBuffer.rewind();

            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(headerBuffer);

            fileStorage = new FileStorage(
                    filename,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initMaps();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return fileStorage;
    }

    private void initMaps()
    {
        try
        {
            final long numberOfMaps = (channel.size() / fileDbHeader.memoryMappedChunkSizeBytes) + 1;
            for (long i = 0; i < numberOfMaps; i++)
            {
                mapMemory(i * fileDbHeader.memoryMappedChunkSizeBytes);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void mapMemory(final long offset)
    {
        try
        {
            final MappedByteBuffer map = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    offset,
                    fileDbHeader.memoryMappedChunkSizeBytes);

            //try to get file size down after the mapping
//            final long fileSize = FileDbHeader.getSizeBytes()
//                    + (mappedBuffers.size() * fileDbHeader.memoryMappedChunkSizeBytes);
//            channel.truncate(fileSize); //doesn't work on windows

            mappedBuffers.add(map);

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
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
            return MemoryFactory.allocateHeap(fileDbHeader.pageSize, fileDbHeader.byteOrder);
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
        final byte[] nodeSupportArray = node.getSupportByteArrayIfAny();
        if (nodeSupportArray != null)
        {
            long currentFilePosition = -1;
            try
            {
                //TODO actually use pageNumber instead of offset ((pos-header) / pageSize)
                currentFilePosition = channel.position();
                channel.write(ByteBuffer.wrap(nodeSupportArray));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return currentFilePosition;
        }
        else
        {
            throw new RuntimeException("Cannot commit node without support byte array");
        }
    }

    @Override
    public void commitMetadata(final long lastRootPageNumber)
    {
        try
        {
            dbFile.seek(FileDbHeader.getHeaderOffsetForLastRoot());
            //this is big-endind, that is why we need to invert, as the header is always little endian
            dbFile.writeLong(Long.reverseBytes(lastRootPageNumber));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public Memory loadLastRoot()
    {
        long currentRootOffset;
        try
        {
            dbFile.seek(FileDbHeader.getHeaderOffsetForLastRoot());
            //this is big-endind, that is why we need to invert, as the header is always little endian
            currentRootOffset = Long.reverseBytes(dbFile.readLong());
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }

        if (currentRootOffset != NO_CURRENT_ROOT_OFFSET)
        {
            return loadPage(currentRootOffset);
        }

        return null;
    }

    public Memory loadPage(final long pageOffset)
    {
        //TODO: make this search logN
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final long offsetMappedBuffer = i * fileDbHeader.memoryMappedChunkSizeBytes;
            if (pageOffset > offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.mapDirect(
                        mappedBuffer,
                        pageOffset - offsetMappedBuffer,
                        fileDbHeader.pageSize,
                        fileDbHeader.byteOrder);
            }
        }

        return null;
    }

    @Override
    public void flush()
    {
        try
        {
            channel.force(true);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
        dbFile.close();
    }
}
