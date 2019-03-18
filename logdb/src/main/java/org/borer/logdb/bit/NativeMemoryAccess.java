package org.borer.logdb.bit;

import sun.misc.Unsafe;

final class NativeMemoryAccess extends MemoryAccess
{
    private NativeMemoryAccess()
    {
    }

    public static long getLong(final long address)
    {
        return THE_UNSAFE.getLong(address);
    }

    public static void putLong(final long address, final long value)
    {
        THE_UNSAFE.putLong(address, value);
    }

    public static void putInt(final long address, final int value)
    {
        THE_UNSAFE.putInt(address, value);
    }

    public static int getInt(final long address)
    {
        return THE_UNSAFE.getInt(address);
    }

    public static void putBytes(final long destinationAddress, final byte[] sourceArray)
    {
        long srcAdd = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        long dstAdd = destinationAddress;
        long lengthBytes = sourceArray.length;
        while (lengthBytes > 0)
        {
            final long chunk = Math.min(lengthBytes, UNSAFE_COPY_THRESHOLD_BYTES);
            THE_UNSAFE.copyMemory(sourceArray, srcAdd, null, dstAdd, chunk);
            lengthBytes -= chunk;
            srcAdd += chunk;
            dstAdd += chunk;
        }
    }

    public static void getBytes(final long sourceAddress, final byte[] destinationArray)
    {
        //unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        //unsafe.copyMemory(src, dst, size);
        //don't try to get the address of the msgBytes array or any other java object as it may be invalidated by GC
        //unsafe.setMemory(src, srcOffset, size, (byte)val); - set from address to address+size to val -- an easy fill
        long srcAdd = sourceAddress;
        long dstAdd = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        long lengthBytes = destinationArray.length;
        while (lengthBytes > 0)
        {
            final long chunk = Math.min(lengthBytes, UNSAFE_COPY_THRESHOLD_BYTES);
            THE_UNSAFE.copyMemory(null, srcAdd, destinationArray, dstAdd, chunk);
            lengthBytes -= chunk;
            srcAdd += chunk;
            dstAdd += chunk;
        }
    }

    static void copyBytes(
            final Object sourceUnsafeObject,
            final long sourceAddress,
            final Object destinationUnsafeObject,
            final long destinationAddress,
            final long sizeBytes)
    {
        //unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        //unsafe.copyMemory(src, dst, size);
        //don't try to get the address of the msgBytes array or any other java object as it may be invalidated by GC
        //unsafe.setMemory(src, srcOffset, size, (byte)val); - set from address to address+size to val -- an easy fill

        long srcAdd = sourceAddress;
        long dstAdd = destinationAddress;
        long lengthBytes = sizeBytes;
        while (lengthBytes > 0)
        {
            final long chunk = Math.min(lengthBytes, UNSAFE_COPY_THRESHOLD_BYTES);
            THE_UNSAFE.copyMemory(sourceUnsafeObject, srcAdd, destinationUnsafeObject, dstAdd, chunk);
            lengthBytes -= chunk;
            srcAdd += chunk;
            dstAdd += chunk;
        }
    }

    static void putByte(final long address, final byte b)
    {
        THE_UNSAFE.putByte(address, b);
    }

    static byte getByte(final long address)
    {
        return THE_UNSAFE.getByte(address);
    }
}
