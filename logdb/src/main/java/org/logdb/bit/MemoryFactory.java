package org.logdb.bit;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.Objects;

public class MemoryFactory
{
    public static @ByteOffset long getPageOffset(final MappedByteBuffer mappedBuffer, final @ByteOffset long offset)
    {
        Objects.requireNonNull(mappedBuffer, "buffer cannot be null");

        return StorageUnits.offset(MemoryAccess.getBaseAddressForDirectBuffer(mappedBuffer) + offset);
    }

    public static DirectMemory getUninitiatedDirectMemory(final @ByteSize int pageSize, final ByteOrder byteOrder)
    {
        final boolean nativeOrder = MemoryOrder.isNativeOrder(byteOrder);

        if (nativeOrder)
        {
            return new MemoryDirectImpl(pageSize);
        }
        else
        {
            return new MemoryDirectNonNativeImpl(pageSize);
        }
    }

    private static DirectMemory getDirectMemory(final @ByteOffset long baseAddress, final @ByteSize int capacity, final ByteOrder byteOrder)
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

    public static DirectMemory allocateDirect(final @ByteSize int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
        buffer.order(byteOrder);
        final @ByteOffset long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);

        return getDirectMemory(baseAddress, capacity, byteOrder);
    }

    public static HeapMemory allocateHeap(final @ByteSize int capacity, final ByteOrder byteOrder)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.order(byteOrder);

        return new MemoryByteBufferImpl(buffer);
    }
}
