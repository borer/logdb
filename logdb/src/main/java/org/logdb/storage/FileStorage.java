package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.logdb.Config.LOG_DB_VERSION;

public final class FileStorage implements Storage, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private static final int MAX_MAPPED_SIZE = Integer.MAX_VALUE;

    private final List<MappedByteBuffer> mappedBuffers;

    private final RandomAccessFile dbFile;
    private final FileChannel channel;
    private final FileDbHeader fileDbHeader;

    private FileStorage(
            final File file,
            final FileDbHeader fileDbHeader,
            final RandomAccessFile dbFile,
            final FileChannel channel)
    {
        Objects.requireNonNull(file, "db file cannot be null");
        this.fileDbHeader = Objects.requireNonNull(fileDbHeader, "db header cannot be null");
        this.dbFile = Objects.requireNonNull(dbFile, "db file cannot be null");
        this.channel = Objects.requireNonNull(channel, "db file cannel cannot be null");
        this.mappedBuffers = new ArrayList<>();
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

        if (memoryMappedChunkSizeBytes % pageSizeBytes != 0)
        {
            throw new IllegalArgumentException(
                    "Memory mapped chunk size must be multiple of page size. Provided page size was " + pageSizeBytes +
                            " , provided memory maped chunk size was " + memoryMappedChunkSizeBytes);
        }

        LOGGER.info("Creating database file " + file.getAbsolutePath());

        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(file, "rw");
            final FileChannel channel = dbFile.getChannel();

            final FileDbHeader fileDbHeader = new FileDbHeader(
                    byteOrder,
                    LOG_DB_VERSION,
                    pageSizeBytes,
                    memoryMappedChunkSizeBytes,
                    StorageUnits.INVALID_PAGE_NUMBER
            );

            fileDbHeader.writeTo(channel);
            fileDbHeader.alignChannelToHeaderPage(channel);

            fileStorage = new FileStorage(
                    file,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initFromFile();
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

            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(channel);
            fileDbHeader.alignChannelToHeaderPage(channel);

            fileStorage = new FileStorage(
                    file,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initFromFile();
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

    private void initFromFile() throws IOException
    {
        final long fileSize = channel.size();
        final long maxMappedChunks = fileSize / MAX_MAPPED_SIZE;
        final long lastChunkSize = fileSize - (maxMappedChunks * MAX_MAPPED_SIZE);

        for (int i = 0; i < maxMappedChunks; i++)
        {
            mapMemory(i * MAX_MAPPED_SIZE, MAX_MAPPED_SIZE);
        }

        if (lastChunkSize > 0)
        {

            final long mapLength = Math.max(lastChunkSize, fileDbHeader.memoryMappedChunkSizeBytes);
            mapMemory(maxMappedChunks * MAX_MAPPED_SIZE, mapLength);
        }
    }

    private void extendMapsIfRequired()
    {
        try
        {
            final long requiredNumberOfMaps = getRequiredNumberOfMaps();
            long originalChannelPosition = channel.position();

            final int existingRegions = mappedBuffers.size();
            final long regionsToMap = requiredNumberOfMaps - existingRegions;
            for (long i = 0; i < regionsToMap; i++)
            {
                mapMemory((existingRegions + i) * fileDbHeader.memoryMappedChunkSizeBytes, fileDbHeader.memoryMappedChunkSizeBytes);
            }

            channel.position(originalChannelPosition);
        }
        catch (final IOException e)
        {
            LOGGER.error("Couldn't extend the mapped db file", e);
        }
    }

    private void mapMemory(final long offset, final long mapLength)
    {
        try
        {
            //Try to grow the file first as it may crash the VM if it doesn't the mapping doesn't grow it.
            final long newLength = offset + mapLength;
            if (dbFile.length() < newLength)
            {
                dbFile.setLength(newLength);
            }

            final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, offset, mapLength);
            //try to get file size down after the mapping
            //final long fileSize = FileDbHeader.getSizeBytes()
            //        + (mappedBuffers.size() * fileDbHeader.memoryMappedChunkSizeBytes);
            //doesn't work on windows
            //channel.truncate(fileSize);

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
    public HeapMemory allocateHeapPage()
    {
        return MemoryFactory.allocateHeap(fileDbHeader.pageSize, fileDbHeader.byteOrder);
    }

    @Override
    public DirectMemory getUninitiatedDirectMemoryPage()
    {
        return MemoryFactory.getUninitiatedDirectMemory(fileDbHeader.pageSize, fileDbHeader.byteOrder);
    }

    @Override
    public long write(final ByteBuffer buffer)
    {
        assert buffer != null : "buffer to persist must be non null";

        long positionOffset = -1;
        try
        {
            positionOffset = channel.position();
            FileUtils.writeFully(channel, buffer);
            extendMapsIfRequired();
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to persist node to database file. Position offset " + positionOffset, e);
        }

        return positionOffset;
    }

    @Override
    public @PageNumber long writePageAligned(final ByteBuffer buffer)
    {
        assert buffer.capacity() == fileDbHeader.pageSize
                : "buffer must be of page size " + fileDbHeader.pageSize +
                        " capacity. Current buffer capacity " + buffer.capacity();

        final long positionOffset = write(buffer);
        return StorageUnits.pageNumber(positionOffset / fileDbHeader.pageSize);
    }

    @Override
    public void commitMetadata(final @PageNumber long lastRootPageNumber, final @Version long version)
    {
        try
        {
            fileDbHeader.updateMeta(lastRootPageNumber, version);
            fileDbHeader.writeMeta(dbFile);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to persist metadata", e);
        }
    }

    @Override
    public @PageNumber long getLastRootPageNumber()
    {
        return fileDbHeader.getLastRootOffset();
    }

    private long getRequiredNumberOfMaps() throws IOException
    {
        return (channel.position() / fileDbHeader.memoryMappedChunkSizeBytes) + 1;
    }

    @Override
    public DirectMemory loadPage(final @PageNumber long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        long offsetMappedBuffer = 0;
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final long pageOffset = pageNumber * fileDbHeader.pageSize;
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.mapDirect(
                        mappedBuffer,
                        pageOffset - offsetMappedBuffer,
                        fileDbHeader.pageSize,
                        fileDbHeader.byteOrder);
            }

            offsetMappedBuffer += mappedBuffer.limit();
        }

        return null;
    }

    @Override
    public long getBaseOffsetForPageNumber(final @PageNumber long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        assert pageNumber >= 0 : "Page Number can only be positive. Provided " + pageNumber;
        long offsetMappedBuffer = 0;
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final long pageOffset = pageNumber * fileDbHeader.pageSize;
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.getPageOffset(mappedBuffer, pageOffset - offsetMappedBuffer);
            }

            offsetMappedBuffer += mappedBuffer.limit();
        }

        return -1;
    }

    @Override
    public long getPageSize()
    {
        return fileDbHeader.pageSize;
    }

    @Override
    public @PageNumber long getPageNumber(final long offset)
    {
        return StorageUnits.pageNumber(offset / fileDbHeader.pageSize);
    }

    @Override
    public long getOffset(final @PageNumber long pageNumber)
    {
        return pageNumber * fileDbHeader.pageSize;
    }

    @Override
    public void flush()
    {
        try
        {
            channel.force(true);
            extendMapsIfRequired();
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
