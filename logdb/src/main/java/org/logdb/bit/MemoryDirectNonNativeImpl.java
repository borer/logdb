package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.nio.ByteOrder;

import static org.logdb.storage.StorageUnits.BYTE_OFFSET;
import static org.logdb.storage.StorageUnits.BYTE_SIZE;
import static org.logdb.storage.StorageUnits.INT_BYTES_OFFSET;
import static org.logdb.storage.StorageUnits.INT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.LONG_BYTES_OFFSET;
import static org.logdb.storage.StorageUnits.LONG_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.SHORT_BYTES_SIZE;
import static org.logdb.storage.StorageUnits.ZERO_OFFSET;

public class MemoryDirectNonNativeImpl implements DirectMemory
{
    private static final @ByteOffset long UNINITIALIZED_ADDRESS = StorageUnits.offset(Long.MIN_VALUE);

    private @ByteSize long capacity;
    private @ByteOffset long baseAddress;
    private @ByteOffset long position;
    private boolean isInitialized;

    MemoryDirectNonNativeImpl(final @ByteSize int capacity)
    {
        this(UNINITIALIZED_ADDRESS, capacity, false);
    }

    MemoryDirectNonNativeImpl(final @ByteOffset long baseAddress, final @ByteSize long capacity)
    {
        this(baseAddress, capacity, true);
    }

    private MemoryDirectNonNativeImpl(final @ByteOffset long baseAddress, final @ByteSize long capacity, final boolean isInitialized)
    {
        this.baseAddress = baseAddress;
        this.capacity = capacity;
        this.position = ZERO_OFFSET;
        this.isInitialized = isInitialized;
    }

    @Override
    public @ByteOffset long getBaseAddress()
    {
        return baseAddress;
    }

    @Override
    public void setBaseAddress(final @ByteOffset long baseAddress)
    {
        this.isInitialized = baseAddress != UNINITIALIZED_ADDRESS;
        this.baseAddress = baseAddress;
    }

    @Override
    public boolean isInitialized()
    {
        return isInitialized;
    }

    @Override
    public ByteOrder getByteOrder()
    {
        return MemoryOrder.nonNativeOrder;
    }

    @Override
    public void resetPosition()
    {
        position = StorageUnits.ZERO_OFFSET;
    }

    @Override
    public @ByteSize long getCapacity()
    {
        return capacity;
    }

    @Override
    public Memory slice(final @ByteOffset int startOffset)
    {
        return sliceRangeInternal(startOffset, StorageUnits.offset(capacity));
    }

    @Override
    public Memory sliceRange(@ByteOffset int startOffset, @ByteOffset int endOffset)
    {
        return sliceRangeInternal(startOffset, endOffset);
    }

    private  Memory sliceRangeInternal(final @ByteOffset int startOffset, final @ByteOffset long endOffset)
    {
        final @ByteSize long newCapacity = StorageUnits.size(endOffset - startOffset);

        return new MemoryDirectNonNativeImpl(baseAddress + startOffset, newCapacity, isInitialized);
    }

    @Override
    public void putLong(final long value)
    {
        assertBounds(position, LONG_BYTES_SIZE);

        putLong(position, value);
        position += LONG_BYTES_OFFSET;
    }

    @Override
    public void putLong(final @ByteOffset long offset, final long value)
    {
        assertBounds(offset, LONG_BYTES_SIZE);

        NonNativeMemoryAccess.putLong(baseAddress + offset, value);
    }

    @Override
    public long getLong()
    {
        return getLong(position);
    }

    @Override
    public long getLong(final @ByteOffset long offset)
    {
        assertBounds(offset, LONG_BYTES_SIZE);

        return NonNativeMemoryAccess.getLong(baseAddress + offset);
    }

    @Override
    public void putInt(final int value)
    {
        assertBounds(position, INT_BYTES_SIZE);

        putInt(position, value);
        position += INT_BYTES_OFFSET;
    }

    @Override
    public void putInt(final @ByteOffset long offset, final int value)
    {
        assertBounds(offset, INT_BYTES_SIZE);

        NonNativeMemoryAccess.putInt(baseAddress + offset, value);
    }

    @Override
    public void reset()
    {
        NonNativeMemoryAccess.fillBytes(baseAddress, capacity, (byte)0);
    }

    @Override
    public int getInt()
    {
        return getInt(position);
    }

    @Override
    public int getInt(final @ByteOffset long offset)
    {
        assertBounds(offset, INT_BYTES_SIZE);

        return NonNativeMemoryAccess.getInt(baseAddress + offset);
    }

    @Override
    public short getShort(final @ByteOffset long offset)
    {
        assertBounds(offset, SHORT_BYTES_SIZE);

        return NonNativeMemoryAccess.getShort(baseAddress + offset);
    }

    @Override
    public void getBytes(final byte[] destinationArray)
    {
        getBytes(position, StorageUnits.size(destinationArray.length), destinationArray);
    }

    @Override
    public void getBytes(final @ByteSize long length, final byte[] destinationArray)
    {
        getBytes(position, length, destinationArray);
    }

    @Override
    public void getBytes(final @ByteOffset long offset, final @ByteSize long length, final byte[] destinationArray)
    {
        assertBounds(offset, length);

        NonNativeMemoryAccess.getBytes(
                baseAddress + offset,
                destinationArray,
                ZERO_OFFSET,
                StorageUnits.size(destinationArray.length));
    }

    @Override
    public void getBytes(
            final @ByteOffset long offset,
            final @ByteSize long length,
            final byte[] destinationArray,
            final @ByteOffset long destinationArrayOffset)
    {
        assertBounds(offset, StorageUnits.size(destinationArray.length));

        NonNativeMemoryAccess.getBytes(
                baseAddress + offset,
                destinationArray,
                destinationArrayOffset,
                length);
    }

    @Override
    public void putBytes(final byte[] sourceArray)
    {
        assertBounds(position, StorageUnits.size(sourceArray.length));

        putBytes(position, sourceArray);
        position += StorageUnits.offset(sourceArray.length);
    }

    @Override
    public void putBytes(final @ByteOffset long destinationOffset, final byte[] sourceArray)
    {
        assertBounds(destinationOffset, StorageUnits.size(sourceArray.length));

        NonNativeMemoryAccess.putBytes(baseAddress + destinationOffset, sourceArray);
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
        assertBounds(offset, BYTE_SIZE);

        NonNativeMemoryAccess.putByte(baseAddress + offset, b);
    }

    @Override
    public byte getByte()
    {
        return getByte(position);
    }

    @Override
    public byte getByte(final @ByteOffset long offset)
    {
        assertBounds(baseAddress + offset, BYTE_SIZE);

        return NonNativeMemoryAccess.getByte(baseAddress + offset);
    }

    @Override
    public void putShort(final @ByteOffset long offset, final short value)
    {
        assertBounds(offset, SHORT_BYTES_SIZE);

        NonNativeMemoryAccess.putShort(baseAddress + offset, value);
    }

    @Override
    public void assertBounds(final @ByteOffset long requestOffset, final @ByteSize int requestLength)
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
    public void assertBounds(final @ByteOffset long requestOffset, final @ByteSize long requestLength)
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
