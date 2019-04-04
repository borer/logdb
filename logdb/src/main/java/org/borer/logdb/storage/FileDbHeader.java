package org.borer.logdb.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

public class FileDbHeader
{
    private static byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDB
    private static int BYTE_ORDER_OFFSET = LOG_DB_MAGIC_STRING.length; // size 7
    private static int VERSION_OFFSET = BYTE_ORDER_OFFSET + 1; // size 7 + 1 = 8
    private static int PAGE_SIZE_OFFSET = VERSION_OFFSET + Integer.BYTES;
    private static int MEMORY_MAPPED_CHUNK_SIZE_OFFSET = PAGE_SIZE_OFFSET + Integer.BYTES;
    private static int LAST_ROOT_OFFSET = MEMORY_MAPPED_CHUNK_SIZE_OFFSET + Long.BYTES;

    private final int version;
    private final long lastRootOffset;

    final ByteOrder byteOrder;
    final int pageSize; // Must be a power of two
    final int headerSizeInPages;
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
        this.headerSizeInPages = (FileDbHeader.getSizeBytes() / pageSize) + 1;
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        this.lastRootOffset = lastRootOffset;
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

    static FileDbHeader readFrom(final ByteBuffer buffer)
    {
        final byte[] magicString = new byte[LOG_DB_MAGIC_STRING.length];
        buffer.get(magicString);

        if (!Arrays.equals(LOG_DB_MAGIC_STRING, magicString))
        {
            throw new RuntimeException("DB file is not valid");
        }

        final ByteOrder byteOrder = getByteOrder(buffer.get(BYTE_ORDER_OFFSET));
        final int version = buffer.getInt(VERSION_OFFSET);
        final int pageSize = buffer.getInt(PAGE_SIZE_OFFSET);
        final long memoryMappedChunkSizeBytes = buffer.getLong(MEMORY_MAPPED_CHUNK_SIZE_OFFSET);
        final long lastRootOffset = buffer.getLong(LAST_ROOT_OFFSET);

        return new FileDbHeader(byteOrder, version, pageSize, memoryMappedChunkSizeBytes, lastRootOffset);
    }

    void writeTo(final ByteBuffer buffer)
    {
        buffer.put(LOG_DB_MAGIC_STRING);
        buffer.put(BYTE_ORDER_OFFSET, getEncodedByteOrder(byteOrder));
        buffer.putInt(VERSION_OFFSET, version);
        buffer.putInt(PAGE_SIZE_OFFSET, pageSize);
        buffer.putLong(MEMORY_MAPPED_CHUNK_SIZE_OFFSET, memoryMappedChunkSizeBytes);
        buffer.putLong(LAST_ROOT_OFFSET, lastRootOffset);
    }

    static long getHeaderOffsetForLastRoot()
    {
        return LAST_ROOT_OFFSET;
    }

    private static byte getEncodedByteOrder(final ByteOrder byteOrder)
    {
        return (byte) ((byteOrder == ByteOrder.BIG_ENDIAN) ? 1 : 0);
    }

    private static ByteOrder getByteOrder(final byte byteOrderEncoded)
    {
        return byteOrderEncoded == 1 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    static int getSizeBytes()
    {
        return LAST_ROOT_OFFSET + Long.BYTES;
    }
}
