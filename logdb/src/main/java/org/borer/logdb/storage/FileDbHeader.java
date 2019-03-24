package org.borer.logdb.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

public class FileDbHeader
{
    private static byte[] LOG_DB_MAGIC_STRING = { 0x4c, 0x6f, 0x67, 0x44, 0x42, 0x00, 0x00}; // LogDB

    final ByteOrder byteOrder;
    private final int version;
    final int pageSize; // Must be a power of two
    final long memoryMappedChunkSizeBytes;
    private final long currentRootOffset;

    FileDbHeader(
            final ByteOrder byteOrder,
            final int version,
            final int pageSize,
            final long memoryMappedChunkSizeBytes,
            final long currentRootOffset)
    {
        this.byteOrder = byteOrder;
        this.version = version;
        this.pageSize = pageSize;
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        this.currentRootOffset = currentRootOffset;
    }

    @Override
    public String toString()
    {
        return "FileDbHeader{"
                + "byteOrder=" + byteOrder
                + ", version=" + version
                + ", pageSize=" + pageSize
                + ", memoryMappedChunkSizeBytes=" + memoryMappedChunkSizeBytes
                + ", currentRootOffset=" + currentRootOffset
                + '}';
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
        return version == that.version
                && pageSize == that.pageSize
                && memoryMappedChunkSizeBytes == that.memoryMappedChunkSizeBytes
                && currentRootOffset == that.currentRootOffset
                && byteOrder.equals(that.byteOrder);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(byteOrder, version, pageSize, memoryMappedChunkSizeBytes, currentRootOffset);
    }

    static FileDbHeader readFrom(final ByteBuffer buffer)
    {
        final byte[] magicString = new byte[LOG_DB_MAGIC_STRING.length];
        buffer.get(magicString);

        if (!Arrays.equals(LOG_DB_MAGIC_STRING, magicString))
        {
            throw new RuntimeException("DB file is not valid");
        }

        final ByteOrder byteOrder = getByteOrder(buffer.get());
        final int version = buffer.getInt();
        final int pageSize = buffer.getInt();
        final long memoryMappedChunkSizeBytes = buffer.getLong();
        final long currentRootOffset = buffer.getLong();

        return new FileDbHeader(byteOrder, version, pageSize, memoryMappedChunkSizeBytes, currentRootOffset);
    }

    void writeTo(final ByteBuffer buffer)
    {
        buffer.put(LOG_DB_MAGIC_STRING);
        buffer.put(getEncodedByteOrder(byteOrder));
        buffer.putInt(version);
        buffer.putInt(pageSize);
        buffer.putLong(memoryMappedChunkSizeBytes);
        buffer.putLong(currentRootOffset);
    }

    static long getHeaderOffsetForCurrentRoot()
    {
        return getSizeBytes() - Long.BYTES;
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
        return LOG_DB_MAGIC_STRING.length // size 7
                + Byte.BYTES // byte order // 7 + 1 = 8
                + Integer.BYTES //version
                + Integer.BYTES //page size bytes
                + Long.BYTES // memory mapped chunk size bytes
                + Long.BYTES; // current root offset
    }
}
