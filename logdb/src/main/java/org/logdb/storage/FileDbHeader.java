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
    private static int PAGE_SIZE_OFFSET = BYTE_ORDER_OFFSET + Byte.BYTES; // size 7 + 1 = 8
    private static int MEMORY_MAPPED_CHUNK_SIZE_OFFSET = PAGE_SIZE_OFFSET + Integer.BYTES;
    private static int VERSION_OFFSET = MEMORY_MAPPED_CHUNK_SIZE_OFFSET + Long.BYTES;
    private static int LAST_ROOT_OFFSET = VERSION_OFFSET + Long.BYTES;

    static int HEADER_SIZE = LAST_ROOT_OFFSET + Long.BYTES;

    private final int headerSizeInPages;

    final int version;
    final long lastRootOffset;
    final ByteOrder byteOrder;
    final int pageSize; // Must be a power of two
    final long memoryMappedChunkSizeBytes;

    FileDbHeader(
            final ByteOrder byteOrder,
            final int version,
            final int pageSize,
            final long memoryMappedChunkSizeBytes,
            final long lastRootOffset)
    {
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
        final int version = getIntegerInCorrectByteOrder(buffer.getInt(VERSION_OFFSET));
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
        buffer.putInt(VERSION_OFFSET, getIntegerInCorrectByteOrder(version));
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

    void updateMeta(final RandomAccessFile file, final long newRootPageNumber, final long version) throws IOException
    {
        final long currentFilePosition = file.getFilePointer();
        file.seek(FileDbHeader.VERSION_OFFSET);
        //RandomAccessFile.writeLong always writes the long in big endian. that is why we need to invert it.
        file.writeLong(Long.reverseBytes(version));
        file.writeLong(Long.reverseBytes(newRootPageNumber));
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
        return (byte) ((byteOrder == ByteOrder.BIG_ENDIAN) ? 1 : 0);
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
