package org.logdb.storage.file;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.INVALID_OFFSET;
import static org.logdb.storage.file.FileStorageHeader.HEADER_OFFSET;

public final class FileStorage implements Storage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private final List<MappedByteBuffer> mappedBuffers;
    private final FileHeader fileStorageHeader;
    private final FileHeader newFileStorageHeader;
    private final FileAllocator fileAllocator;

    //cached values from the file header
    private final ByteOrder order;
    private final @ByteSize long fileSegmentSize;
    private final @ByteSize int pageSize;

    private RandomAccessFile currentAppendingFile;
    private FileChannel currentAppendingChannel;
    private @ByteOffset long globalFilePosition;

    FileStorage(
            final Path rootDirectory,
            final FileAllocator fileAllocator,
            final FileHeader fileStorageHeader,
            final FileHeader newFileStorageHeader, 
            final RandomAccessFile currentAppendingFile,
            final FileChannel currentAppendingChannel,
            final List<MappedByteBuffer> mappedBuffers,
            final @ByteOffset long globalFilePosition)
    {
        Objects.requireNonNull(rootDirectory, "Database root directory cannot be null");
        this.fileAllocator = Objects.requireNonNull(fileAllocator, "filename strategy cannot be null");
        this.fileStorageHeader = Objects.requireNonNull(fileStorageHeader, "db header cannot be null");
        this.currentAppendingFile = Objects.requireNonNull(currentAppendingFile, "db file cannot be null");
        this.currentAppendingChannel = Objects.requireNonNull(currentAppendingChannel, "db file currentAppendingChannel cannot be null");
        this.mappedBuffers = mappedBuffers;
        this.globalFilePosition = globalFilePosition;
        this.newFileStorageHeader = newFileStorageHeader;
        
        this.order = fileStorageHeader.getOrder();
        this.fileSegmentSize = fileStorageHeader.getSegmentFileSize();
        this.pageSize = fileStorageHeader.getPageSize();
    }

    private @ByteOffset long tryRollCurrentFile(final @ByteSize int nextWriteSize) throws IOException
    {
        try
        {
            final @ByteOffset long originalChannelPosition = StorageUnits.offset(currentAppendingChannel.position());
            final @ByteSize long expectedFileSize = StorageUnits.size(originalChannelPosition + nextWriteSize);
            final boolean shouldRollFile = expectedFileSize > fileSegmentSize;
            if (shouldRollFile)
            {

                final @ByteOffset long spaceLeftInCurrentSegmentFile =
                        StorageUnits.offset(fileSegmentSize - originalChannelPosition);
                globalFilePosition += spaceLeftInCurrentSegmentFile;

                currentAppendingFile.close();

                createAndMapNewFile();
            }
        }
        catch (final IOException e)
        {
            final String msg = "Couldn't extend the mapped db file";
            LOGGER.error(msg, e);
            throw new IOException(msg, e);
        }

        return globalFilePosition;
    }

    private void createAndMapNewFile() throws IOException
    {
        final File file = fileAllocator.generateNextFile();
        final RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        final FileChannel channel = accessFile.getChannel();
        accessFile.setLength(fileSegmentSize);


        //TODO: have an object that implements file header interface
        newFileStorageHeader.writeToAndPageAlign(channel);
        globalFilePosition += StorageUnits.offset(channel.position());

        currentAppendingFile = accessFile;
        currentAppendingChannel = channel;

        final MappedByteBuffer mappedByteBuffer = mapFile(channel, order);
        mappedBuffers.add(mappedByteBuffer);
    }

    static MappedByteBuffer mapFile(final FileChannel channel, final ByteOrder byteOrder) throws IOException
    {
        final @ByteSize long size = StorageUnits.size(channel.size());
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);

        map.order(byteOrder);

        return map;
    }

    @Override
    public HeapMemory allocateHeapPage()
    {
        return MemoryFactory.allocateHeap(pageSize, order);
    }

    @Override
    public DirectMemory getUninitiatedDirectMemoryPage()
    {
        return MemoryFactory.getUninitiatedDirectMemory(pageSize, order);
    }

    @Override
    public @ByteOffset long append(final ByteBuffer buffer) throws IOException
    {
        assert buffer != null : "buffer to persist must be non null";

        if (!order.equals(buffer.order()))
        {
            buffer.order(order);
        }

        @ByteOffset long positionOffset = INVALID_OFFSET;
        try
        {
            final @ByteSize int writeSize = StorageUnits.size(buffer.capacity());
            positionOffset = tryRollCurrentFile(writeSize);
            FileUtils.writeFully(currentAppendingChannel, buffer);
            globalFilePosition += StorageUnits.offset(writeSize);
        }
        catch (final IOException e)
        {
            final String msg = "Unable to persist node to database file. Position offset " + positionOffset;
            LOGGER.error(msg, e);
            throw new IOException(msg, e);
        }

        return positionOffset;
    }

    @Override
    public @PageNumber long appendPageAligned(final ByteBuffer buffer) throws IOException
    {
        assert (buffer.capacity() % pageSize) == 0
                : "buffer must be of multiple of page size " + pageSize +
                        " capacity. Current buffer capacity " + buffer.capacity();

        final @ByteOffset long positionOffset = append(buffer);
        return StorageUnits.pageNumber(positionOffset / pageSize);
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, final @Version long version)
    {
        try
        {
            final @ByteOffset long appendOffset = StorageUnits.offset(currentAppendingChannel.position());
            fileStorageHeader.updateMeta(lastPersistedOffset, appendOffset, version);

            final long originalChannelPosition = currentAppendingChannel.position();
            currentAppendingChannel.position(HEADER_OFFSET);

            //TODO: have an object that implements file header interface
            fileStorageHeader.writeTo(currentAppendingChannel);

            currentAppendingChannel.position(originalChannelPosition);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to persist metadata", e);
        }
    }

    @Override
    public @ByteOffset long getLastPersistedOffset()
    {
        return fileStorageHeader.getGlobalAppendOffset();
    }

    @Override
    public @Version long getAppendVersion()
    {
        return fileStorageHeader.getAppendVersion();
    }

    @Override
    public void mapPage(final @PageNumber long pageNumber, final DirectMemory memory)
    {
        final @ByteOffset long baseOffset = getBaseOffset(pageNumber);
        memory.setBaseAddress(baseOffset);
    }

    private @ByteOffset long getBaseOffset(final @PageNumber long pageNumber)
    {
        assert pageNumber >= 0 : "Page Number can only be positive. Provided " + pageNumber;
        assert pageNumber <= getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileSegmentSize))
                : "The page number " + pageNumber + " is outside the mapped range of " +
                getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileSegmentSize));

        @ByteOffset long offsetMappedBuffer = StorageUnits.ZERO_OFFSET;
        final @ByteOffset long pageOffset = StorageUnits.offset(pageNumber * pageSize);

        //TODO: make this search logN (use a structure of (offsetStart,buffer) and then binary search on offset)
        for (int i = 0; i < mappedBuffers.size(); i++)
        {
            final MappedByteBuffer mappedBuffer = mappedBuffers.get(i);
            final @ByteOffset long mappedBufferEndOffset = StorageUnits.offset(offsetMappedBuffer + mappedBuffer.limit());
            if (pageOffset >= offsetMappedBuffer && pageOffset < mappedBufferEndOffset)
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
        return pageSize;
    }

    @Override
    public ByteOrder getOrder()
    {
        return order;
    }

    @Override
    public @PageNumber long getPageNumber(final @ByteOffset long offset)
    {
        return StorageUnits.pageNumber(offset / pageSize);
    }

    @Override
    public @ByteOffset long getOffset(final @PageNumber long pageNumber)
    {
        return StorageUnits.offset(pageNumber * pageSize);
    }

    @Override
    public void flush()
    {
        try
        {
            currentAppendingChannel.force(false);
            fileStorageHeader.flush();
            newFileStorageHeader.flush();
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to flush persistence layer", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        mappedBuffers.clear();
        currentAppendingChannel.close();
        currentAppendingFile.close();

        if (fileStorageHeader instanceof FixedFileStorageHeader)
        {
            ((FixedFileStorageHeader) fileStorageHeader).close();
        }

        if (newFileStorageHeader instanceof FixedFileStorageHeader)
        {
            ((FixedFileStorageHeader) newFileStorageHeader).close();
        }

        //force unmapping of the file, TODO: find another method to trigger the unmapping
        System.gc();
    }
}
