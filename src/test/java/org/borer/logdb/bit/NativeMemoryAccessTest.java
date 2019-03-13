package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class NativeMemoryAccessTest
{
    @Test
    void shouldPutAndGetLongIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final long expected = 14789L;

        NativeMemoryAccess.putLong(addressForByteBuffer, expected);
        final long actual = NativeMemoryAccess.getLong(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final int expected = 98765;

        NativeMemoryAccess.putInt(addressForByteBuffer, expected);
        final int actual = NativeMemoryAccess.getInt(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetBytesIntoDirectAllocatedMemory()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final ByteBuffer buffer = ByteBuffer.allocateDirect(expected.length);
        final long addressForByteBuffer = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);

        NativeMemoryAccess.putBytes(addressForByteBuffer, expected);
        NativeMemoryAccess.getBytes(addressForByteBuffer, actual);

        for (int i = 0; i < expected.length; i++)
        {
            assertEquals(expected[i], actual[i]);
        }

        assertEquals(expectedMsg, new String(actual));
    }

    @Test
    void shouldNotMatchNativeAndNonNativeByteOrder()
    {
        final long target = 14789L;

        final ByteBuffer bufferNative = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBufferNative = NativeMemoryAccess.getBaseAddressForDirectBuffer(bufferNative);
        NativeMemoryAccess.putLong(addressForByteBufferNative, target);
        final long actualNonNative = NativeMemoryAccess.getLong(addressForByteBufferNative);

        final long addressForByteBufferNonNative = NonNativeMemoryAccess.getBaseAddressForDirectBuffer(bufferNative);
        final long actualNative = NonNativeMemoryAccess.getLong(addressForByteBufferNonNative);

        assertNotEquals(actualNative, actualNonNative);
    }
}