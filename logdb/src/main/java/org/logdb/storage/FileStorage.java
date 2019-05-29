package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.bit.ReadMemory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
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
import java.util.Objects;
import java.util.Queue;

import static org.logdb.Config.LOG_DB_VERSION;

public final class FileStorage implements Storage, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final int NO_CURRENT_ROOT_PAGE_NUMBER = -1;

    private final File file;
    private final List<MappedByteBuffer> mappedBuffers;
    private final Queue<HeapMemory> availableWritableMemory;

    private final RandomAccessFile dbFile;
    private final FileChannel channel;
    private final FileDbHeader fileDbHeader;

    private FileStorage(
            final File file,
            final FileDbHeader fileDbHeader,
            final RandomAccessFile dbFile,
            final FileChannel channel)
    {
        this.file = Objects.requireNonNull(file, "db file cannot be null");
        this.fileDbHeader = Objects.requireNonNull(fileDbHeader, "db header cannot be null");
        this.dbFile = Objects.requireNonNull(dbFile, "db file cannot be null");
        this.channel = Objects.requireNonNull(channel, "db file cannel cannot be null");
        this.mappedBuffers = new ArrayList<>();
        this.availableWritableMemory = new ArrayDeque<>();
    }

    public static FileStorage createNewFileDb(
            final File file,
            final long memoryMappedChunkSizeBytes,
            final ByteOrder byteOrder,
            final int pageSizeBytes)
    {
        Objects.requireNonNull(file, "Db file cannot be null");

        if (pageSizeBytes < 128 || pageSizeBytes % 2 != 0)
        {
            throw new IllegalArgumentException("Page Size must be bigger than 128 bytes and a power of 2. Provided was " + pageSizeBytes);
        }

        LOGGER.info("Creating database file " + file.getAbsolutePath());

        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(file, "rw");
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

            FileUtils.writeFully(channel, headerBuffer);
            channel.position(fileDbHeader.headerSizeInPages * pageSizeBytes);

            fileStorage = new FileStorage(
                    file,
                    fileDbHeader,
                    dbFile,
                    channel);

            final long requiredNumberOfMaps = getRequiredFileMapsForWholeFile(fileDbHeader.memoryMappedChunkSizeBytes, channel.size());
            fileStorage.extendMapsIfRequired(requiredNumberOfMaps);
        }
        catch (final FileNotFoundException e)
        {
            LOGGER.error("Unable to find db file " + file.getAbsolutePath(), e);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to read/write to db file " + file.getAbsolutePath(), e);
        }

        return fileStorage;
    }

    public static FileStorage openDbFile(final File file)
    {
        Objects.requireNonNull(file, "DB file cannot be null");

        LOGGER.info("Loading database file " + file.getAbsolutePath());

        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(file, "rw");
            final FileChannel channel = dbFile.getChannel();

            final ByteBuffer headerBuffer = ByteBuffer.allocate(FileDbHeader.getSizeBytes());
            headerBuffer.order(DEFAULT_HEADER_BYTE_ORDER);
            FileUtils.readFully(channel, headerBuffer, 0);
            headerBuffer.rewind();

            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(headerBuffer);
            channel.position(fileDbHeader.headerSizeInPages * fileDbHeader.pageSize);

            fileStorage = new FileStorage(
                    file,
                    fileDbHeader,
                    dbFile,
                    channel);

            final long requiredNumberOfMaps = getRequiredFileMapsForWholeFile(fileDbHeader.memoryMappedChunkSizeBytes, channel.size());
            fileStorage.extendMapsIfRequired(requiredNumberOfMaps);
        }
        catch (final FileNotFoundException e)
        {
            LOGGER.error("Unable to find db file " + file.getAbsolutePath(), e);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to read from db file " + file.getAbsolutePath(), e);
        }

        return fileStorage;
    }

    private static long getRequiredFileMapsForWholeFile(final long memoryMappedChunkSizeBytes, final long fileSize)
    {
        return (fileSize / memoryMappedChunkSizeBytes) + 1;
    }

    private void extendMapsIfRequired(final long requiredNumberOfMaps)
    {
        try
        {
            long originalChannelPosition = channel.position();

            final int existingRegions = mappedBuffers.size();
            final long regionsToMap = requiredNumberOfMaps - existingRegions;
            for (long i = 0; i < regionsToMap; i++)
            {
                mapMemory((existingRegions + i) * fileDbHeader.memoryMappedChunkSizeBytes);
            }

            channel.position(originalChannelPosition);
        }
        catch (final IOException e)
        {
            LOGGER.error("Couldn't extend the mapped db file", e);
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
        catch (final IOException e)
        {
            LOGGER.error(
                    "Couldn't mapped db file at offset " + offset +
                            "for " + fileDbHeader.memoryMappedChunkSizeBytes + " bytes", e);
        }
    }

    @Override
    public HeapMemory allocateHeapMemory()
    {
        final HeapMemory writableMemory = availableWritableMemory.poll();
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
    public long writeNode(final ReadMemory node)
    {
        final ByteBuffer nodeSupportBuffer = node.getSupportByteBufferIfAny();
        if (nodeSupportBuffer != null)
        {
            long pageNumber = -1;
            try
            {
                pageNumber = channel.position() / fileDbHeader.pageSize;
                FileUtils.writeFully(channel, nodeSupportBuffer);
                extendMapsIfRequired(getRequiredNumberOfMaps());
            }
            catch (final IOException e)
            {
                LOGGER.error("Unable to persist node to database file. Page number " + pageNumber, e);
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
        try
        {
            final long currentChanelPosition = channel.position();
            dbFile.seek(FileDbHeader.getHeaderOffsetForLastRoot());
            //this is big-endind, that is why we need to invert, as the header is always little endian
            dbFile.writeLong(Long.reverseBytes(lastRootPageNumber));
            channel.position(currentChanelPosition);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to persist metadata", e);
        }
    }

    @Override
    public long getLastRootPageNumber()
    {
        return fileDbHeader.lastRootOffset;
    }

    private long getRequiredNumberOfMaps() throws IOException
    {
        return (channel.position() / fileDbHeader.memoryMappedChunkSizeBytes) + 1;
    }

    @Override
    public DirectMemory loadPage(final long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
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
    public long getBaseOffsetForPageNumber(final long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        assert pageNumber >= 0 : "Page Number can only be positive";
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final long offsetMappedBuffer = i * fileDbHeader.memoryMappedChunkSizeBytes;
            final long pageOffset = pageNumber * fileDbHeader.pageSize;
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.getPageOffset(mappedBuffer, pageOffset - offsetMappedBuffer);
            }
        }

        return -1;
    }

    public DirectMemory getDirectMemory(final long pageNumber)
    {
        final long baseOffsetForPageNumber = getBaseOffsetForPageNumber(pageNumber);
        return MemoryFactory.getGetDirectMemory(
                baseOffsetForPageNumber,
                fileDbHeader.pageSize,
                fileDbHeader.byteOrder);
    }

    @Override
    public long getPageSize()
    {
        return fileDbHeader.pageSize;
    }

    @Override
    public void flush()
    {
        try
        {
            channel.force(true);
            extendMapsIfRequired(getRequiredNumberOfMaps());
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to flush persistence layer", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        mappedBuffers.clear();
        channel.close();
        dbFile.close();

        //force unmapping of the file, TODO: find another method to trigger the unmapping
        System.gc();
    }
}
