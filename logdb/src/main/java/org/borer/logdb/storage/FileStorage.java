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
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.borer.logdb.Config.LOG_DB_VERSION;

public final class FileStorage implements Storage, Closeable
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    public static final int NO_CURRENT_ROOT_PAGE_NUMBER = -1;

    private final String filename;
    private final List<MappedByteBuffer> mappedBuffers;
    private final Queue<Memory> availableWritableMemory;

    private final RandomAccessFile dbFile;
    private final FileChannel channel;
    private final FileDbHeader fileDbHeader;
    private long lastRootPageNumber;

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
        this.lastRootPageNumber = -1;
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
                    NO_CURRENT_ROOT_PAGE_NUMBER
            );

            headerBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
            fileDbHeader.writeTo(headerBuffer);
            headerBuffer.rewind();

            channel.write(headerBuffer);
            channel.force(true);
            channel.position(fileDbHeader.headerSizeInPages * pageSizeBytes);

            fileStorage = new FileStorage(
                    filename,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.extendMapsIfRequired();
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
            headerBuffer.rewind();

            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(headerBuffer);
            channel.position(fileDbHeader.headerSizeInPages * fileDbHeader.pageSize);

            fileStorage = new FileStorage(
                    filename,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.extendMapsIfRequired();
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

    private void extendMapsIfRequired()
    {
        try
        {
            long originalChannelPosition = channel.position();

            final long requiredNumberOfMaps = calculateRequiredNumberOfMapMemoryRegions();
            final long regionsToMap = (requiredNumberOfMaps == 0 ? 1 : requiredNumberOfMaps) - mappedBuffers.size();
            for (long i = 0; i < regionsToMap; i++)
            {
                mapMemory(i * fileDbHeader.memoryMappedChunkSizeBytes);
            }

            channel.position(originalChannelPosition);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private long calculateRequiredNumberOfMapMemoryRegions() throws IOException
    {
        return channel.size() / fileDbHeader.memoryMappedChunkSizeBytes;
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
        final byte[] byteArray = writableMemory.getSupportByteArrayIfAny();
        if (byteArray != null)
        {
            //Only reset the byte array for heap memories
            Arrays.fill(byteArray, (byte) 0);
        }

        availableWritableMemory.add(writableMemory);
    }

    @Override
    public long commitNode(final Memory node)
    {
        final byte[] nodeSupportArray = node.getSupportByteArrayIfAny();
        if (nodeSupportArray != null)
        {
            long pageNumber = -1;
            try
            {
                pageNumber = channel.position() / fileDbHeader.pageSize;
                channel.write(ByteBuffer.wrap(nodeSupportArray));
                extendMapsIfRequired();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return pageNumber;
        }
        else
        {
            throw new RuntimeException("Cannot commit node without support byte array");
        }
    }

    @Override
    public void commitMetadata(final long lastRootPageNumber)
    {
        this.lastRootPageNumber = lastRootPageNumber;
        try
        {
            final long currentChanelPosition = channel.position();
            dbFile.seek(FileDbHeader.getHeaderOffsetForLastRoot());
            //this is big-endind, that is why we need to invert, as the header is always little endian
            dbFile.writeLong(Long.reverseBytes(lastRootPageNumber));
            channel.position(currentChanelPosition);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public long getLastRootPageNumber()
    {
        return lastRootPageNumber;
    }

    @Override
    public Memory loadLastRoot()
    {
        long currentRootPageNumber;
        try
        {
            final long currentChanelPosition = channel.position();
            dbFile.seek(FileDbHeader.getHeaderOffsetForLastRoot());
            //this is big-endind, that is why we need to invert, as the header is always little endian
            currentRootPageNumber = Long.reverseBytes(dbFile.readLong());
            channel.position(currentChanelPosition);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }

        if (currentRootPageNumber != NO_CURRENT_ROOT_PAGE_NUMBER)
        {
            return loadPage(currentRootPageNumber);
        }

        return null;
    }

    public Memory loadPage(final long pageNumber)
    {
        //TODO: make this search logN
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final long offsetMappedBuffer = i * fileDbHeader.memoryMappedChunkSizeBytes;
            final long pageOffset = pageNumber * fileDbHeader.pageSize;
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
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
            extendMapsIfRequired();
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
