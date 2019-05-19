package org.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class NonNativeMemoryAccessTest
{
    @Test
    void shouldPutAndGetLongIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = NonNativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final long expected = 14789L;

        NonNativeMemoryAccess.putLong(addressForByteBuffer, expected);
        final long actual = NonNativeMemoryAccess.getLong(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = NonNativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final int expected = 98765;

        NonNativeMemoryAccess.putInt(addressForByteBuffer, expected);
        final int actual = NonNativeMemoryAccess.getInt(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetBytesIntoDirectAllocatedMemory()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final ByteBuffer buffer = ByteBuffer.allocateDirect(expected.length);
        final long addressForByteBuffer = NonNativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);

        NonNativeMemoryAccess.putBytes(addressForByteBuffer, expected);
        NonNativeMemoryAccess.getBytes(addressForByteBuffer, actual);

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

        final ByteBuffer bufferNonNative = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBufferNonNative = NonNativeMemoryAccess.getBaseAddressForDirectBuffer(bufferNonNative);
        NonNativeMemoryAccess.putLong(addressForByteBufferNonNative, target);
        final long actualNonNative = NonNativeMemoryAccess.getLong(addressForByteBufferNonNative);

        final long addressForByteBufferNative = NativeMemoryAccess.getBaseAddressForDirectBuffer(bufferNonNative);
        final long actualNative = NativeMemoryAccess.getLong(addressForByteBufferNative);

        assertEquals(target, actualNonNative);
        assertNotEquals(actualNative, actualNonNative);
    }
}