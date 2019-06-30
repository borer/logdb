package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryByteBufferImpl implements HeapMemory
{
    private final ByteBuffer buffer;

    MemoryByteBufferImpl(final ByteBuffer buffer)
    {
        assert buffer != null && !buffer.isDirect() : "buffer has to be heap allocated and not null";
        this.buffer = Objects.requireNonNull(buffer, "buffer cannot be null");
    }

    @Override
    public ByteBuffer getSupportByteBufferIfAny()
    {
        return buffer;
    }

    @Override
    public @ByteOffset long getBaseAddress()
    {
        return StorageUnits.offset(Unsafe.ARRAY_BYTE_BASE_OFFSET);
    }

    @Override
    public ByteOrder getByteOrder()
    {
        return buffer.order();
    }

    @Override
    public void resetPosition()
    {
        buffer.position(ZERO_OFFSET);
    }

    @Override
    public @ByteSize long getCapacity()
    {
        return StorageUnits.size(buffer.capacity());
    }

    @Override
    public void putLong(final long value)
    {
        buffer.putLong(value);
    }

    @Override
    public void putLong(final @ByteOffset long offset, final long value)
    {
        buffer.putLong((int)offset, value);
    }

    @Override
    public long getLong()
    {
        return buffer.getLong();
    }

    @Override
    public long getLong(final @ByteOffset long offset)
    {
        return buffer.getLong((int)offset);
    }

    @Override
    public void putInt(final int value)
    {
        buffer.putInt(value);
    }

    @Override
    public void putInt(final @ByteOffset long offset, final int value)
    {
        buffer.putInt((int)offset, value);
    }

    @Override
    public int getInt()
    {
        return buffer.getInt();
    }

    @Override
    public int getInt(final @ByteOffset long offset)
    {
        return buffer.getInt((int)offset);
    }

    @Override
    public void getBytes(final byte[] destinationArray)
    {
        buffer.get(destinationArray);
    }

    @Override
    public void getBytes(final @ByteSize long length, final byte[] destinationArray)
    {
        buffer.get(destinationArray, ZERO_OFFSET, (int)length);
    }

    @Override
    public void getBytes(
            final @ByteOffset long offset,
            final @ByteSize long length,
            final byte[] destinationArray)
    {
        System.arraycopy(buffer.array(), (int)offset, destinationArray, ZERO_OFFSET, (int)length);
    }

    @Override
    public void getBytes(
            final @ByteOffset long offset,
            final @ByteSize long length,
            final byte[] destinationArray,
            final @ByteOffset long destinationArrayOffset)
    {
        System.arraycopy(
                buffer.array(),
                (int)offset,
                destinationArray,
                (int)destinationArrayOffset,
                (int)length);
    }

    @Override
    public void putBytes(byte[] sourceArray)
    {
        buffer.put(sourceArray);
    }

    @Override
    public void putBytes(@ByteOffset long destinationOffset, final byte[] sourceArray)
    {
        System.arraycopy(sourceArray, ZERO_OFFSET, buffer.array(), (int)destinationOffset, sourceArray.length);
    }

    @Override
    public void putByte(final byte b)
    {
        buffer.put(b);
    }

    @Override
    public void putByte(final @ByteOffset long offset, final byte b)
    {
        buffer.put((int)offset, b);
    }

    @Override
    public byte getByte()
    {
        return buffer.get();
    }

    @Override
    public byte getByte(final @ByteOffset long offset)
    {
        return buffer.get((int) offset);
    }

    @Override
    public void assertBounds(final @ByteOffset long requestOffset, final @ByteSize int requestLength)
    {
        assert ((requestOffset | requestLength | (requestOffset + requestLength) |
                (buffer.capacity() - (requestOffset + requestLength))) >= 0)
                : "requestOffset: " + requestOffset + ", requestLength: " + requestLength +
                    ", (requestOffset + requestLength): " + (requestOffset + requestLength) +
                    ", allocSize: " + buffer.capacity();
    }

    @Override
    public void assertBounds(final @ByteOffset long requestOffset, final @ByteSize long requestLength)
    {
        assert ((requestOffset | requestLength | (requestOffset + requestLength) |
                (buffer.capacity() - (requestOffset + requestLength))) >= 0)
                    : "requestOffset: " + requestOffset + ", requestLength: " + requestLength +
                        ", (requestOffset + requestLength): " + (requestOffset + requestLength) +
                        ", allocSize: " + buffer.capacity();
    }
}
