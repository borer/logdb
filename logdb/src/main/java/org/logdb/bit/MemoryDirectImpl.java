package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.StorageUnits;

import java.nio.ByteBuffer;

import static org.logdb.storage.StorageUnits.BYTE_OFFSET;
import static org.logdb.storage.StorageUnits.INT_BYTES_OFFSET;
import static org.logdb.storage.StorageUnits.LONG_BYTES_OFFSET;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryDirectImpl implements DirectMemory
{
    private static final @ByteOffset long UNINITIALIZED_ADDRESS = StorageUnits.offset(Long.MIN_VALUE);

    private final long capacity;

    private @ByteOffset long baseAddress;
    private @ByteOffset long position;

    MemoryDirectImpl(final long capacity)
    {
        this(UNINITIALIZED_ADDRESS, capacity);
    }

    MemoryDirectImpl(final @ByteOffset long baseAddress, final long capacity)
    {
        this.baseAddress = baseAddress;
        this.capacity = capacity;
        this.position = ZERO_OFFSET;
    }

    @Override
    public ByteBuffer getSupportByteBufferIfAny()
    {
        return null;
    }

    @Override
    public @ByteOffset long getBaseAddress()
    {
        return baseAddress;
    }

    @Override
    public void setBaseAddress(final @ByteOffset long baseAddress)
    {
        this.baseAddress = baseAddress;
    }

    @Override
    public void resetPosition()
    {
        position = StorageUnits.ZERO_OFFSET;
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
        position += LONG_BYTES_OFFSET;
    }

    @Override
    public void putLong(final @ByteOffset long offset, final long value)
    {
        assertBounds(offset, Long.BYTES);

        NativeMemoryAccess.putLong(baseAddress + offset, value);
    }

    @Override
    public long getLong()
    {
        return getLong(position);
    }

    @Override
    public long getLong(final @ByteOffset long offset)
    {
        assertBounds(offset, Long.BYTES);

        return NativeMemoryAccess.getLong(baseAddress + offset);
    }

    @Override
    public void putInt(final int value)
    {
        assertBounds(position, Integer.BYTES);

        putInt(position, value);
        position += INT_BYTES_OFFSET;
    }

    @Override
    public void putInt(final @ByteOffset long offset, final int value)
    {
        assertBounds(offset, Integer.BYTES);

        NativeMemoryAccess.putInt(baseAddress + offset, value);
    }

    @Override
    public int getInt()
    {
        return getInt(position);
    }

    @Override
    public int getInt(final @ByteOffset long offset)
    {
        assertBounds(offset, Integer.BYTES);

        return NativeMemoryAccess.getInt(baseAddress + offset);
    }

    @Override
    public void getBytes(final byte[] destinationArray)
    {
        getBytes(position, destinationArray.length, destinationArray);
    }

    @Override
    public void getBytes(final @ByteOffset long length, final byte[] destinationArray)
    {
        getBytes(position, length, destinationArray);
    }

    @Override
    public void getBytes(final @ByteOffset long offset, final long length, final byte[] destinationArray)
    {
        assertBounds(offset, StorageUnits.offset(destinationArray.length));

        NativeMemoryAccess.getBytes(baseAddress + offset, destinationArray, ZERO_OFFSET, length);
    }

    @Override
    public void getBytes(
            final @ByteOffset long offset,
            final long length,
            final byte[] destinationArray,
            final @ByteOffset long destinationArrayOffset)
    {
        assertBounds(offset, StorageUnits.offset(destinationArray.length));

        NativeMemoryAccess.getBytes(
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
        position += StorageUnits.offset(sourceArray.length);
    }

    @Override
    public void putBytes(final @ByteOffset long destinationOffset, final byte[] sourceArray)
    {
        assertBounds(destinationOffset, sourceArray.length);

        NativeMemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
    }

    @Override
    public void putByte(final byte b)
    {
        putByte(position, b);
        position += BYTE_OFFSET;
    }

    @Override
    public void putByte(final @ByteOffset long offset, final byte b)
    {
        assertBounds(offset, Byte.BYTES);

        NativeMemoryAccess.putByte(baseAddress + offset, b);
    }

    @Override
    public byte getByte()
    {
        return getByte(position);
    }

    @Override
    public byte getByte(final @ByteOffset long offset)
    {
        assertBounds(baseAddress + offset, Byte.BYTES);

        return NativeMemoryAccess.getByte(baseAddress + offset);
    }

    @Override
    public void assertBounds(final @ByteOffset long requestOffset, final int requestLength)
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
    public void assertBounds(final @ByteOffset long requestOffset, final long requestLength)
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
