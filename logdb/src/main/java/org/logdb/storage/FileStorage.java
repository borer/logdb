package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static org.logdb.storage.StorageUnits.INVALID_OFFSET;

public final class FileStorage implements Storage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private static final @ByteSize int MAX_MAPPED_SIZE = StorageUnits.size(Integer.MAX_VALUE);

    private final List<MappedByteBuffer> mappedBuffers;

    private final RandomAccessFile dbFile;
    private final FileChannel channel;
    private final FileDbHeader fileDbHeader;

    private @ByteSize long currentMapSize;

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
        this.currentMapSize = StorageUnits.ZERO_SIZE;
    }

    public static FileStorage createNewFileDb(
            final File file,
            final @ByteSize long memoryMappedChunkSizeBytes,
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes)
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
                    StorageUnits.INVALID_OFFSET
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
        final @ByteSize long fileSize = StorageUnits.size(channel.size());
        final @ByteSize long maxMappedChunks = StorageUnits.size(fileSize / MAX_MAPPED_SIZE);
        final @ByteSize long lastChunkSize = StorageUnits.size(fileSize - (maxMappedChunks * MAX_MAPPED_SIZE));

        for (int i = 0; i < maxMappedChunks; i++)
        {
            mapMemory(StorageUnits.offset(i * MAX_MAPPED_SIZE), MAX_MAPPED_SIZE);
        }

        if (lastChunkSize > 0)
        {

            final @ByteSize long mapLength = StorageUnits.size(Math.max(lastChunkSize, fileDbHeader.memoryMappedChunkSizeBytes));
            mapMemory(StorageUnits.offset(maxMappedChunks * MAX_MAPPED_SIZE), mapLength);
            currentMapSize = mapLength;
        }

        extendMapsIfRequired();

        final @ByteOffset long lastPersistedOffset = fileDbHeader.getLastPersistedOffset();
        if (lastPersistedOffset > 0)
        {
            channel.position(lastPersistedOffset);
        }
    }

    private void extendMapsIfRequired()
    {
        try
        {
            final @ByteOffset long originalChannelPosition = StorageUnits.offset(channel.position());
            final @ByteSize long differenceBetweenMappedAndCurrentFileSize = StorageUnits.size(originalChannelPosition - currentMapSize);
            if (differenceBetweenMappedAndCurrentFileSize <= 0)
            {
                return;
            }

            final @ByteSize long extendSize = StorageUnits.size(
                    Math.max(
                            fileDbHeader.memoryMappedChunkSizeBytes,
                            differenceBetweenMappedAndCurrentFileSize));

            mapMemory(StorageUnits.offset(currentMapSize), extendSize);

            channel.position(originalChannelPosition);

            currentMapSize += extendSize;
        }
        catch (final IOException e)
        {
            LOGGER.error("Couldn't extend the mapped db file", e);
        }
    }

    private void mapMemory(final @ByteOffset long offset, final @ByteSize long mapLength)
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
            map.order(fileDbHeader.byteOrder);
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
    public @ByteOffset long write(final ByteBuffer buffer)
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

        return StorageUnits.offset(positionOffset);
    }

    @Override
    public @PageNumber long writePageAligned(final ByteBuffer buffer)
    {
        assert buffer.capacity() == fileDbHeader.pageSize
                : "buffer must be of page size " + fileDbHeader.pageSize +
                        " capacity. Current buffer capacity " + buffer.capacity();

        final @ByteOffset long positionOffset = write(buffer);
        return StorageUnits.pageNumber(positionOffset / fileDbHeader.pageSize);
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, final @Version long version)
    {
        try
        {
            fileDbHeader.updateMeta(lastPersistedOffset, version);
            fileDbHeader.writeMeta(dbFile);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to persist metadata", e);
        }
    }

    @Override
    public @ByteOffset long getLastPersistedOffset()
    {
        return fileDbHeader.getLastPersistedOffset();
    }

    @Override
    public DirectMemory loadPage(final @PageNumber long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        @ByteOffset long offsetMappedBuffer = StorageUnits.ZERO_OFFSET;
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final @ByteOffset long pageOffset = StorageUnits.offset(pageNumber * fileDbHeader.pageSize);
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.mapDirect(
                        mappedBuffer,
                        pageOffset - offsetMappedBuffer,
                        fileDbHeader.pageSize,
                        fileDbHeader.byteOrder);
            }

            offsetMappedBuffer += StorageUnits.offset(mappedBuffer.limit());
        }

        return null;
    }

    @Override
    public @ByteOffset long getBaseOffsetForPageNumber(final @PageNumber long pageNumber)
    {
        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        assert pageNumber >= 0 : "Page Number can only be positive. Provided " + pageNumber;
        @ByteOffset long offsetMappedBuffer = StorageUnits.ZERO_OFFSET;
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final @ByteOffset long pageOffset = StorageUnits.offset(pageNumber * fileDbHeader.pageSize);
            if (pageOffset >= offsetMappedBuffer && pageOffset < (offsetMappedBuffer + mappedBuffer.limit()))
            {
                return MemoryFactory.getPageOffset(mappedBuffer, pageOffset - offsetMappedBuffer);
            }

            offsetMappedBuffer += StorageUnits.offset(mappedBuffer.limit());
        }

        return INVALID_OFFSET;
    }

    @Override
    public @ByteSize long getPageSize()
    {
        return fileDbHeader.pageSize;
    }

    @Override
    public ByteOrder getOrder()
    {
        return fileDbHeader.byteOrder;
    }

    @Override
    public @PageNumber long getPageNumber(final @ByteOffset long offset)
    {
        return StorageUnits.pageNumber(offset / fileDbHeader.pageSize);
    }

    @Override
    public @ByteOffset long getOffset(final @PageNumber long pageNumber)
    {
        return StorageUnits.offset(pageNumber * fileDbHeader.pageSize);
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
