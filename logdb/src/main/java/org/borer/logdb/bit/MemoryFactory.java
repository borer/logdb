package org.borer.logdb.bit;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MemoryFactory
{
    public static Memory allocateDirect(final int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);

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

    public static Memory allocateHeap(final byte[] array, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.order(byteOrder);

        return new MemoryByteBufferImpl(buffer);
    }

    public static Memory allocateHeap(final int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.order(byteOrder);

        return new MemoryByteBufferImpl(buffer);
    }
}
