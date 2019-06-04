package org.logdb.bit;

import java.nio.ByteBuffer;

public class MemoryDirectNonNativeImpl implements DirectMemory
{
    private static final long UNINITIALIZED_ADDRESS = Long.MIN_VALUE;

    private final long capacity;

    private long baseAddress;
    private long position;

    MemoryDirectNonNativeImpl(final int capacity)
    {
        this(UNINITIALIZED_ADDRESS, capacity);
    }

    MemoryDirectNonNativeImpl(final long baseAddress, final long capacity)
    {
        this.baseAddress = baseAddress;
        this.capacity = capacity;
        this.position = 0;
    }

    @Override
    public ByteBuffer getSupportByteBufferIfAny()
    {
        return null;
    }

    @Override
    public long getBaseAddress()
    {
        return baseAddress;
    }

    @Override
    public void setBaseAddress(final long baseAddress)
    {
        this.baseAddress = baseAddress;
    }

    @Override
    public void resetPosition()
    {
        position = 0;
    }

    @Override
    public long getCapacity()
    {
        return capacity;
    }

    @Override
    public void putLong(final long value)
    {
        assertBounds(position, Long.BYTES);

        putLong(position, value);
        position += Long.BYTES;
    }

    @Override
    public void putLong(final long offset, final long value)
    {
        assertBounds(offset, Long.BYTES);

        NonNativeMemoryAccess.putLong(baseAddress + offset, value);
    }

    @Override
    public long getLong()
    {
        return getLong(position);
    }

    @Override
    public long getLong(final long offset)
    {
        assertBounds(offset, Long.BYTES);

        return NonNativeMemoryAccess.getLong(baseAddress + offset);
    }

    @Override
    public void putInt(final int value)
    {
        assertBounds(position, Integer.BYTES);

        putInt(position, value);
        position += Integer.BYTES;
    }

    @Override
    public void putInt(final long offset, final int value)
    {
        assertBounds(offset, Integer.BYTES);

        NonNativeMemoryAccess.putInt(baseAddress + offset, value);
    }

    @Override
    public int getInt()
    {
        return getInt(position);
    }

    @Override
    public int getInt(final long offset)
    {
        assertBounds(offset, Integer.BYTES);

        return NonNativeMemoryAccess.getInt(baseAddress + offset);
    }

    @Override
    public void getBytes(final byte[] destinationArray)
    {
        getBytes(position, destinationArray.length, destinationArray);
    }

    @Override
    public void getBytes(final long length, final byte[] destinationArray)
    {
        getBytes(position, length, destinationArray);
    }

    @Override
    public void getBytes(final long offset, final long length, final byte[] destinationArray)
    {
        assertBounds(offset, length);

        NonNativeMemoryAccess.getBytes(
                baseAddress + offset,
                destinationArray,
                0,
                destinationArray.length);
    }

    @Override
    public void getBytes(long offset, long length, byte[] destinationArray, long destinationArrayOffset)
    {
        assertBounds(offset, destinationArray.length);

        NonNativeMemoryAccess.getBytes(
                baseAddress + offset,
                destinationArray,
                destinationArrayOffset,
                length);
    }

    @Override
    public void putBytes(final byte[] sourceArray)
    {
        assertBounds(position, sourceArray.length);

        putBytes(position, sourceArray);
        position += sourceArray.length;
    }

    @Override
    public void putBytes(final long destinationOffset, final byte[] sourceArray)
    {
        assertBounds(destinationOffset, sourceArray.length);

        NonNativeMemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }

    @Override
    public void putByte(final byte b)
    {
        putByte(position, b);
        position += Byte.BYTES;
    }

    @Override
    public void putByte(final long offset, final byte b)
    {
        assertBounds(offset, Byte.BYTES);

        NonNativeMemoryAccess.putByte(baseAddress + offset, b);
    }

    @Override
    public byte getByte()
    {
        return getByte(position);
    }

    @Override
    public byte getByte(final long offset)
    {
        assertBounds(baseAddress + offset, Byte.BYTES);

        return NonNativeMemoryAccess.getByte(baseAddress + offset);
    }

    @Override
    public void assertBounds(final long requestOffset, final int requestLength)
    {
        assert baseAddress != UNINITIALIZED_ADDRESS : "Base address is uninitialized";

        assert ((requestOffset |
                requestLength |
                (requestOffset + requestLength) |
                (capacity - (requestOffset + requestLength - baseAddress))) >= 0)
                    : "requestOffset: " + requestOffset + ", requestLength: " + requestLength +
                        ", (requestOffset + requestLength): " + (requestOffset + requestLength) + ", allocSize: " + capacity;
    }

    @Override
    public void assertBounds(final long requestOffset, final long requestLength)
    {
        assert baseAddress != UNINITIALIZED_ADDRESS : "Base address is uninitialized";

        assert ((requestOffset |
                requestLength |
                (requestOffset + requestLength) |
                (capacity - (requestOffset + requestLength - baseAddress))) >= 0)
                    : "requestOffset: " + requestOffset + ", requestLength: " + requestLength +
                    ", (requestOffset + requestLength): " + (requestOffset + requestLength) + ", allocSize: " + capacity;
    }
}
