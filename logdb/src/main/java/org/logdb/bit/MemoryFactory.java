package org.logdb.bit;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.Objects;

public class MemoryFactory
{
    public static Memory mapDirect(
            final MappedByteBuffer mappedBuffer,
            final long offset,
            final int capacity,
            final ByteOrder byteOrder)
    {
        Objects.requireNonNull(mappedBuffer, "buffer cannot be null");

        return getGetDirectMemory(getPageOffset(mappedBuffer, offset), capacity, byteOrder).toMemory();
    }

    public static long getPageOffset(final MappedByteBuffer mappedBuffer, long offset)
    {
        Objects.requireNonNull(mappedBuffer, "buffer cannot be null");

        return NativeMemoryAccess.getBaseAddressForDirectBuffer(mappedBuffer) + offset;
    }

    public static DirectMemory getGetDirectMemory(final long baseAddress, final int capacity, final ByteOrder byteOrder)
    {
        final boolean nativeOrder = MemoryOrder.isNativeOrder(byteOrder);

        if (nativeOrder)
        {
            return new MemoryDirectImpl(baseAddress, capacity);
        }
        else
        {
            return new MemoryDirectNonNativeImpl(baseAddress, capacity);
        }
    }

    public static Memory allocateDirect(final int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);

        return getGetDirectMemory(baseAddress, capacity, byteOrder).toMemory();
    }

    public static Memory allocateHeap(final int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.order(byteOrder);

        return new MemoryByteBufferImpl(buffer);
    }
}
