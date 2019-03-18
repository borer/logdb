package org.borer.logdb.bit;

import sun.misc.Unsafe;

final class NonNativeMemoryAccess extends MemoryAccess
{
    private NonNativeMemoryAccess()
    {
    }

    public static long getLong(final long address)
    {
        return Long.reverseBytes(THE_UNSAFE.getLong(address));
    }

    public static void putLong(final long address, final long value)
    {
        THE_UNSAFE.putLong(address, Long.reverseBytes(value));
    }

    public static void putInt(final long address, final int value)
    {
        THE_UNSAFE.putInt(address, Integer.reverseBytes(value));
    }

    public static int getInt(final long address)
    {
        return Integer.reverseBytes(THE_UNSAFE.getInt(address));
    }

    public static void putBytes(final long destinationAddress, final byte[] sourceArray)
    {
        long srcAdd = Unsafe.ARRAY_BYTE_BASE_OFFSET;
        long dstAdd = destinationAddress;
        final byte[] reverseSourceArray = reverse(sourceArray);
        long lengthBytes = reverseSourceArray.length;
        while (lengthBytes > 0)
        {
            final long chunk = Math.min(lengthBytes, UNSAFE_COPY_THRESHOLD_BYTES);
            THE_UNSAFE.copyMemory(reverseSourceArray, srcAdd, null, dstAdd, chunk);
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

        reverseInPlace(destinationArray);
    }

    private static void reverseInPlace(final byte[] array)
    {
        for (int i = 0, j = array.length - 1; i < j; i++, j--)
        {
            final byte tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
    }

    private static byte[] reverse(final byte[] array)
    {
        final byte[] reverseArray = new byte[array.length];
        for (int i = 0, j = array.length - 1; i < j; i++, j--)
        {
            final byte tmp = array[i];
            reverseArray[i] = array[j];
            reverseArray[j] = tmp;
        }

        return reverseArray;
    }

    public static void putByte(final long address, final byte b)
    {
        THE_UNSAFE.putByte(address, b);
    }

    public static byte getByte(final long address)
    {
        return THE_UNSAFE.getByte(address);
    }
}
