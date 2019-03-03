package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryAccessTest
{
    @Test
    void shouldPutAndGetLongIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = MemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final long expected = 14789L;

        MemoryAccess.putLong(addressForByteBuffer, expected);
        final long actual = MemoryAccess.getLong(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long addressForByteBuffer = MemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final int expected = 98765;

        MemoryAccess.putInt(addressForByteBuffer, expected);
        final int actual = MemoryAccess.getInt(addressForByteBuffer);

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetBytesIntoDirectAllocatedMemory()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final ByteBuffer buffer = ByteBuffer.allocateDirect(expected.length);
        final long addressForByteBuffer = MemoryAccess.getBaseAddressForDirectBuffer(buffer);

        MemoryAccess.putBytes(addressForByteBuffer, expected);
        MemoryAccess.getBytes(addressForByteBuffer, actual);

        for (int i = 0; i < expected.length; i++)
        {
            assertEquals(expected[i], actual[i]);
        }

        assertEquals(expectedMsg, new String(actual));
    }
}