package org.logdb.storage;

import org.logdb.bit.MemoryOrder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Objects;

public class FileDbHeader
{
    private static final ByteOrder DEFAULT_HEADER_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDB
    private static int BYTE_ORDER_OFFSET = LOG_DB_MAGIC_STRING.length; // size 7
    private static int BYTE_ORDER_SIZE = Byte.BYTES; // size 1

    private static int PAGE_SIZE_OFFSET = BYTE_ORDER_OFFSET + BYTE_ORDER_SIZE; // size 7 + 1 = 8
    private static int PAGE_SIZE_BYTES = Integer.BYTES;

    private static int MEMORY_MAPPED_CHUNK_SIZE_OFFSET = PAGE_SIZE_OFFSET + PAGE_SIZE_BYTES;
    private static int MEMORY_MAPPED_CHUNK_SIZE_BYTES = Long.BYTES;

    private static int VERSION_OFFSET = MEMORY_MAPPED_CHUNK_SIZE_OFFSET + MEMORY_MAPPED_CHUNK_SIZE_BYTES;
    private static int VERSION_SIZE = Long.BYTES;

    private static int LAST_ROOT_OFFSET = VERSION_OFFSET + VERSION_SIZE;
    private static int LAST_ROOT_SIZE = Long.BYTES;

    static int HEADER_SIZE = LOG_DB_MAGIC_STRING.length +
            BYTE_ORDER_SIZE +
            PAGE_SIZE_BYTES +
            MEMORY_MAPPED_CHUNK_SIZE_BYTES +
            VERSION_SIZE +
            LAST_ROOT_SIZE;

    private final int headerSizeInPages;

    private long version;
    private long lastRootOffset;

    final ByteOrder byteOrder;
    final int pageSize; // Must be a power of two
    final long memoryMappedChunkSizeBytes; //must be multiple of pageSize

    FileDbHeader(
            final ByteOrder byteOrder,
            final long version,
            final int pageSize,
            final long memoryMappedChunkSizeBytes,
            final long lastRootOffset)
    {
        assert pageSize > 0 && ((pageSize & (pageSize - 1)) == 0) : "page size must be power of 2. Provided " + pageSize;
        assert memoryMappedChunkSizeBytes % pageSize == 0 : "memoryMappedChunkSizeBytes must be multiple of pageSize";

        this.byteOrder = byteOrder;
        this.version = version;
        this.pageSize = pageSize;
        this.headerSizeInPages = (HEADER_SIZE / pageSize) + 1;
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        this.lastRootOffset = lastRootOffset;
    }

    static FileDbHeader readFrom(final ReadableByteChannel channel) throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        FileUtils.readFully(channel, buffer);
        buffer.order(DEFAULT_HEADER_BYTE_ORDER);
        buffer.rewind();

        final byte[] magicString = new byte[LOG_DB_MAGIC_STRING.length];
        buffer.get(magicString);

        if (!Arrays.equals(LOG_DB_MAGIC_STRING, magicString))
        {
            throw new RuntimeException("DB file is not valid");
        }

        final ByteOrder byteOrder = getByteOrder(buffer.get(BYTE_ORDER_OFFSET));
        final long version = getLongInCorrectByteOrder(buffer.getLong(VERSION_OFFSET));
        final int pageSize = getIntegerInCorrectByteOrder(buffer.getInt(PAGE_SIZE_OFFSET));
        final long memoryMappedChunkSizeBytes = getLongInCorrectByteOrder(buffer.getLong(MEMORY_MAPPED_CHUNK_SIZE_OFFSET));
        final long lastRootOffset = getLongInCorrectByteOrder(buffer.getLong(LAST_ROOT_OFFSET));

        return new FileDbHeader(byteOrder, version, pageSize, memoryMappedChunkSizeBytes, lastRootOffset);
    }

    void writeTo(final WritableByteChannel channel) throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);

        buffer.order(DEFAULT_HEADER_BYTE_ORDER);

        buffer.put(LOG_DB_MAGIC_STRING);
        buffer.put(BYTE_ORDER_OFFSET, getEncodedByteOrder(byteOrder));
        buffer.putLong(VERSION_OFFSET, getLongInCorrectByteOrder(version));
        buffer.putInt(PAGE_SIZE_OFFSET, getIntegerInCorrectByteOrder(pageSize));
        buffer.putLong(MEMORY_MAPPED_CHUNK_SIZE_OFFSET, getLongInCorrectByteOrder(memoryMappedChunkSizeBytes));
        buffer.putLong(LAST_ROOT_OFFSET, getLongInCorrectByteOrder(lastRootOffset));

        buffer.rewind();

        FileUtils.writeFully(channel, buffer);
    }

    void alignChannelToHeaderPage(final SeekableByteChannel channel) throws IOException
    {
        channel.position(headerSizeInPages * pageSize);
    }

    public long getLastRootOffset()
    {
        return lastRootOffset;
    }

    void updateMeta(final long newRootPageNumber, final long version)
    {
        this.lastRootOffset = newRootPageNumber;
        this.version = version;
    }

    void writeMeta(final RandomAccessFile file) throws IOException
    {
        final long currentFilePosition = file.getFilePointer();
        file.seek(FileDbHeader.VERSION_OFFSET);
        //RandomAccessFile.writeLong always writes the long in big endian. that is why we need to invert it.
        file.writeLong(Long.reverseBytes(version));
        file.writeLong(Long.reverseBytes(lastRootOffset));
        file.seek(currentFilePosition);
    }

    private static long getLongInCorrectByteOrder(final long value)
    {
        return MemoryOrder.isNativeOrder(DEFAULT_HEADER_BYTE_ORDER) ? value : Long.reverseBytes(value);
    }

    private static int getIntegerInCorrectByteOrder(final int value)
    {
        return MemoryOrder.isNativeOrder(DEFAULT_HEADER_BYTE_ORDER) ? value : Integer.reverseBytes(value);
    }

    private static byte getEncodedByteOrder(final ByteOrder byteOrder)
    {
        return (byte) ((ByteOrder.BIG_ENDIAN.equals(byteOrder)) ? 1 : 0);
    }

    private static ByteOrder getByteOrder(final byte byteOrderEncoded)
    {
        return byteOrderEncoded == 1 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString()
    {
        return "FileDbHeader{" +
                "version=" + version +
                ", lastRootOffset=" + lastRootOffset +
                ", byteOrder=" + byteOrder +
                ", pageSize=" + pageSize +
                ", headerSizeInPages=" + headerSizeInPages +
                ", memoryMappedChunkSizeBytes=" + memoryMappedChunkSizeBytes +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        FileDbHeader that = (FileDbHeader) o;
        return version == that.version &&
                lastRootOffset == that.lastRootOffset &&
                pageSize == that.pageSize &&
                headerSizeInPages == that.headerSizeInPages &&
                memoryMappedChunkSizeBytes == that.memoryMappedChunkSizeBytes &&
                Objects.equals(byteOrder, that.byteOrder);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, lastRootOffset, byteOrder, pageSize, headerSizeInPages, memoryMappedChunkSizeBytes);
    }
}
