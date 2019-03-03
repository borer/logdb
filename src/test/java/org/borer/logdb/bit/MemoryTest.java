package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryTest
{
    @Test
    void shouldPutAndGetLongIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long baseAddress = MemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new Memory(baseAddress);
        final long expected = 14789L;

        memory.putLong(expected);
        memory.resetPosition();
        final long actual = memory.getLong();

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoDirectAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);
        final long baseAddress = MemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new Memory(baseAddress);
        final int expected = 98765;

        memory.putInt(expected);
        memory.resetPosition();
        final int actual = memory.getInt();

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetBytesIntoDirectAllocatedMemory()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final ByteBuffer buffer = ByteBuffer.allocateDirect(expected.length);
        final long baseAddress = MemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new Memory(baseAddress);

        memory.putBytes(expected);
        memory.resetPosition();
        memory.getBytes(actual);

        for (int i = 0; i < expected.length; i++)
        {
            assertEquals(expected[i], actual[i]);
        }

        assertEquals(expectedMsg, new String(actual));
    }
}