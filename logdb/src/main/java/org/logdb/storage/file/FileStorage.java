package org.logdb.storage.file;

import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.bit.MemoryOrder;
import org.logdb.bit.NativeMemoryAccess;
import org.logdb.bit.NonNativeMemoryAccess;
import org.logdb.bit.UnsafeArrayList;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.file.header.FileHeader;
import org.logdb.storage.file.header.FixedFileStorageHeader;
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
import java.util.Objects;

import static org.logdb.storage.StorageUnits.INVALID_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public final class FileStorage implements Storage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorage.class);

    private final FileHeader fileStorageHeader;
    private final FileHeader newFileStorageHeader;
    private final FileAllocator fileAllocator;
    private final UnsafeArrayList<MappedBuffer> mappedBuffers;

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
            final UnsafeArrayList<MappedBuffer> mappedBuffers,
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

        newFileStorageHeader.writeAlign(channel);
        globalFilePosition += StorageUnits.offset(channel.position());

        currentAppendingFile = accessFile;
        currentAppendingChannel = channel;

        final MappedBuffer mappedBuffer = mapFile(channel, order);
        mappedBuffers.add(mappedBuffer);
    }

    static MappedBuffer mapFile(final FileChannel channel, final ByteOrder byteOrder) throws IOException
    {
        final @ByteSize long size = StorageUnits.size(channel.size());
        final MappedByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, ZERO_OFFSET, size);
        mappedBuffer.order(byteOrder);

        final @ByteOffset long mappedBufferBaseAddress = MemoryFactory.getPageOffset(mappedBuffer, ZERO_OFFSET);
        return new MappedBuffer(mappedBuffer, mappedBufferBaseAddress);
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

        @ByteOffset long appendGlobalOffset = INVALID_OFFSET;
        try
        {
            final @ByteSize int writeSize = StorageUnits.size(buffer.capacity());
            appendGlobalOffset = tryRollCurrentFile(writeSize);
            FileUtils.writeFully(currentAppendingChannel, buffer);
            globalFilePosition += StorageUnits.offset(writeSize);
        }
        catch (final IOException e)
        {
            final String msg = "Unable to persist node to database file. Position offset " + appendGlobalOffset;
            LOGGER.error(msg, e);
            throw new IOException(msg, e);
        }

        return appendGlobalOffset;
    }

    @Override
    public @ByteOffset long append(final byte[] buffer) throws IOException
    {
        assert buffer != null : "buffer to persist must be non null";

        @ByteOffset long appendGlobalOffset = INVALID_OFFSET;
        try
        {
            final @ByteSize int writeSize = StorageUnits.size(buffer.length);
            appendGlobalOffset = tryRollCurrentFile(writeSize);
            currentAppendingFile.write(buffer);
            globalFilePosition += StorageUnits.offset(writeSize);
        }
        catch (final IOException e)
        {
            final String msg = "Unable to persist node to database file. Position offset " + appendGlobalOffset;
            LOGGER.error(msg, e);
            throw new IOException(msg, e);
        }

        return appendGlobalOffset;
    }

    @Override
    public @PageNumber long appendPageAligned(final ByteBuffer buffer) throws IOException
    {
        assert (buffer.capacity() % pageSize) == 0
                : "buffer must be of multiple of page size " + pageSize +
                        " capacity. Current buffer capacity " + buffer.capacity();

        final @ByteOffset long appendGlobalOffset = append(buffer);
        return StorageUnits.pageNumber(appendGlobalOffset / pageSize);
    }

    @Override
    public void commitMetadata(final @ByteOffset long lastPersistedOffset, final @Version long version)
    {
        try
        {
            final @ByteOffset long currentFileAppendOffset = StorageUnits.offset(currentAppendingChannel.position());
            fileStorageHeader.updateMeta(lastPersistedOffset, currentFileAppendOffset, version);

            final @ByteOffset long originalChannelPosition = StorageUnits.offset(currentAppendingChannel.position());
            fileStorageHeader.writeDynamicHeaderTo(currentAppendingChannel);
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
    public @PageNumber long getLastPersistedPageNumber()
    {
        final @ByteOffset long lastGlobalPersistedOffset = fileStorageHeader.getGlobalAppendOffset();
        return lastGlobalPersistedOffset == StorageUnits.INVALID_OFFSET
                ? StorageUnits.INVALID_PAGE_NUMBER
                : getPageNumber(lastGlobalPersistedOffset);
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

    @Override
    public void readBytes(final @ByteOffset long offset, final ByteBuffer destinationBuffer)
    {
        final @ByteSize long lengthBytes = StorageUnits.size(destinationBuffer.capacity());
        @ByteOffset long readPosition = ZERO_OFFSET;

        @PageNumber long pageNumber = getPageNumber(offset);
        @ByteOffset long offsetInsidePage = offset - getOffset(pageNumber);

        final @ByteOffset long pageBaseOffset = getBaseOffset(pageNumber);
        final long sourceAddress = pageBaseOffset + offsetInsidePage;

        @ByteSize long pageLeftSpace = StorageUnits.size(pageSize - offsetInsidePage);
        final @ByteSize long bytesToRead = StorageUnits.size(
                Math.min(pageLeftSpace, lengthBytes));

        readPosition += StorageUnits.offset(bytesToRead);

        readBytesNative(sourceAddress, order, destinationBuffer, ZERO_OFFSET, bytesToRead);

        while (readPosition < lengthBytes)
        {
            //continue reading the header from the beginning of the next page
            pageNumber++;

            final @ByteOffset long pageBaseOffset2 = getBaseOffset(pageNumber);
            final @ByteSize long leftToRead = StorageUnits.size(lengthBytes - readPosition);
            final @ByteSize long bytesToRead2 = StorageUnits.size(Math.min(pageSize, leftToRead));

            readBytesNative(pageBaseOffset2, order, destinationBuffer, readPosition, bytesToRead2);

            readPosition += StorageUnits.offset(bytesToRead2);
        }
    }

    private static void readBytesNative(
            final long sourceAddress,
            final ByteOrder order,
            final ByteBuffer destinationBuffer,
            final @ByteOffset long destinationOffset,
            final @ByteSize long destinationLength)
    {
        if (MemoryOrder.isNativeOrder(order))
        {
            NativeMemoryAccess.getBytes(
                    sourceAddress,
                    destinationBuffer.array(),
                    destinationOffset,
                    destinationLength);
        }
        else
        {
            NonNativeMemoryAccess.getBytes(
                    sourceAddress,
                    destinationBuffer.array(),
                    destinationOffset,
                    destinationLength);
        }
    }

    private @ByteOffset long getBaseOffset(final @PageNumber long pageNumber)
    {
        assert pageNumber >= 0 : "Page Number can only be positive. Provided " + pageNumber;
        assert pageNumber <= getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileSegmentSize))
                : "The page number " + pageNumber + " is outside the mapped range of " +
                getPageNumber(StorageUnits.offset(mappedBuffers.size() * fileSegmentSize));

        final @ByteOffset long pageOffset = getOffset(pageNumber);
        final int containingBufferIndex = (int)(pageOffset / fileSegmentSize);
        final @ByteOffset long bufferOffset = StorageUnits.offset(containingBufferIndex * fileSegmentSize);
        final @ByteOffset long offsetInsideSegment = StorageUnits.offset(pageOffset - bufferOffset);

        return StorageUnits.offset(mappedBuffers.get(containingBufferIndex).address + offsetInsideSegment);
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
    public void flush(final boolean flushMeta)
    {
        try
        {
            currentAppendingChannel.force(flushMeta);
            fileStorageHeader.flush(flushMeta);
            newFileStorageHeader.flush(flushMeta);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to flush persistence layer", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        flush(true);

        mappedBuffers.clean();
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
