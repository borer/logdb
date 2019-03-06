package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryByteBufferImplTest
{
    @Test
    void shouldPutAndGetLongIntoHeapAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        final Memory memory = new MemoryByteBufferImpl(buffer);
        final long expected = 14789L;

        memory.putLong(expected);
        memory.resetPosition();
        final long actual = memory.getLong();

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoHeapAllocatedMemory()
    {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        final Memory memory = new MemoryByteBufferImpl(buffer);
        final int expected = 98765;

        memory.putInt(expected);
        memory.resetPosition();
        final int actual = memory.getInt();

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetBytesIntoHeapAllocatedMemory()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final ByteBuffer buffer = ByteBuffer.allocate(expected.length);
        final Memory memory = new MemoryByteBufferImpl(buffer);

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