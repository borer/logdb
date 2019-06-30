package org.logdb.storage;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
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

public final class FileStorage implements Storage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private final List<MappedByteBuffer> mappedBuffers;
    private final FileDbHeader fileDbHeader;
    private final FileAllocator fileAllocator;
    private final FileDbHeader newFileDbHeader;

    private RandomAccessFile currentAppendingFile;
    private FileChannel currentAppendingChannel;

    private @ByteOffset long globalFilePosition;

    FileStorage(
            final Path rootDirectory,
            final FileAllocator fileAllocator,
            final FileDbHeader fileDbHeader,
            final RandomAccessFile currentAppendingFile,
            final FileChannel currentAppendingChannel,
            final List<MappedByteBuffer> mappedBuffers,
            final @ByteOffset long globalFilePosition)
    {
        Objects.requireNonNull(rootDirectory, "Database root directory cannot be null");
        this.fileAllocator = Objects.requireNonNull(fileAllocator, "filename strategy cannot be null");
        this.fileDbHeader = Objects.requireNonNull(fileDbHeader, "db header cannot be null");
        this.currentAppendingFile = Objects.requireNonNull(currentAppendingFile, "db file cannot be null");
        this.currentAppendingChannel = Objects.requireNonNull(currentAppendingChannel, "db file currentAppendingChannel cannot be null");
        this.mappedBuffers = mappedBuffers;
        this.globalFilePosition = globalFilePosition;
        this.newFileDbHeader = FileDbHeader.newHeader(fileDbHeader.byteOrder, fileDbHeader.pageSize, fileDbHeader.segmentFileSize);
    }

    private void tryRollCurrentFile(final @ByteSize int nextWriteSize)
    {
        try
        {
            final @ByteOffset long originalChannelPosition = StorageUnits.offset(currentAppendingChannel.position());
            final @ByteSize long nextFileSize = StorageUnits.size(originalChannelPosition + nextWriteSize);
            final boolean shouldRollFile = nextFileSize > fileDbHeader.segmentFileSize;
            if (shouldRollFile)
            {

                final @ByteOffset long spaceLeftInCurrentSegmentFile = StorageUnits.offset(fileDbHeader.segmentFileSize - originalChannelPosition);
                globalFilePosition += spaceLeftInCurrentSegmentFile;

                currentAppendingFile.close();

                createAndMapNewFile();
            }
        }
        catch (final IOException e)
        {
            LOGGER.error("Couldn't extend the mapped db file", e);
        }
    }

    private void createAndMapNewFile() throws IOException
    {
        final File file = fileAllocator.generateNextFile();
        final RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        final FileChannel channel = accessFile.getChannel();
        accessFile.setLength(fileDbHeader.segmentFileSize);

        newFileDbHeader.writeTo(channel);
        globalFilePosition += StorageUnits.offset(channel.position());

        currentAppendingFile = accessFile;
        currentAppendingChannel = channel;

        final MappedByteBuffer mappedByteBuffer = mapFile(channel, fileDbHeader.byteOrder);
        mappedBuffers.add(mappedByteBuffer);
    }

    static MappedByteBuffer mapFile(final FileChannel channel, final ByteOrder byteOrder) throws IOException
    {
        final @ByteSize long size = StorageUnits.size(channel.size());
        final MappedByteBuffer map = channel.map(
                FileChannel.MapMode.READ_ONLY,
                0,
                size);

        map.order(byteOrder);

        return map;
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
    public @ByteOffset long append(final ByteBuffer buffer)
    {
        assert buffer != null : "buffer to persist must be non null";

        @ByteOffset long positionOffset = INVALID_OFFSET;
        try
        {
            final @ByteSize int writeSize = StorageUnits.size(buffer.capacity());
            tryRollCurrentFile(writeSize);
            positionOffset = globalFilePosition;
            FileUtils.writeFully(currentAppendingChannel, buffer);
            globalFilePosition += StorageUnits.offset(writeSize);
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
        assert (buffer.capacity() % fileDbHeader.pageSize) == 0
                : "buffer must be of multiple of page size " + fileDbHeader.pageSize +
                        " capacity. Current buffer capacity " + buffer.capacity();

        final @ByteOffset long positionOffset = append(buffer);
        return StorageUnits.pageNumber(positionOffset / fileDbHeader.pageSize);
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, final @Version long version)
    {
        try
        {
            fileDbHeader.updateMeta(
                    lastPersistedOffset,
                    StorageUnits.offset(currentAppendingChannel.position()),
                    version);

            final long originalChannelPosition = currentAppendingChannel.position();
            currentAppendingChannel.position(0);

            fileDbHeader.writeTo(currentAppendingChannel);

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
        return fileDbHeader.getLastPersistedOffset();
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
        assert pageNumber <= getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileDbHeader.segmentFileSize))
                : "The page number " + pageNumber + " is outside the mapped range of " +
                getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileDbHeader.segmentFileSize));

        @ByteOffset long offsetMappedBuffer = StorageUnits.ZERO_OFFSET;
        final @ByteOffset long pageOffset = StorageUnits.offset(pageNumber * fileDbHeader.pageSize);

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
            currentAppendingChannel.force(false);
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
        currentAppendingChannel.close();
        currentAppendingFile.close();

        //force unmapping of the file, TODO: find another method to trigger the unmapping
        System.gc();
    }
}
