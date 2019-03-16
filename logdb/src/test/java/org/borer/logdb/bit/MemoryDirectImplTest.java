package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryDirectImplTest
{
    @Test
    void shouldPutAndGetLongIntoDirectAllocatedMemory()
    {
        final int sizeInBytes = Long.BYTES;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(sizeInBytes);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, sizeInBytes);
        final long expected = 14789L;

        memory.putLong(expected);
        memory.resetPosition();
        final long actual = memory.getLong();

        assertEquals(expected, actual);
    }

    @Test
    void shouldPutAndGetIntIntoDirectAllocatedMemory()
    {
        final int sizeInBytes = Long.BYTES;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(sizeInBytes);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, sizeInBytes);
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
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, expected.length);

        memory.putBytes(expected);
        memory.resetPosition();
        memory.getBytes(actual);

        for (int i = 0; i < expected.length; i++)
        {
            assertEquals(expected[i], actual[i]);
        }

        assertEquals(expectedMsg, new String(actual));
    }

    @Test
    void shouldAssertForInvalidOffsetForInt()
    {
        final int sizeInBytes = Long.BYTES;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(sizeInBytes);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, sizeInBytes);

        try
        {
            memory.putInt(-1, 123);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 4, (requestOffset + requestLength): 3, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.putInt(-1);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 4, (requestOffset + requestLength): 3, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.putInt(Long.BYTES + 1, 123);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 9, requestLength: 4, (requestOffset + requestLength): 13, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.putInt(Long.BYTES + 1);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 9, requestLength: 4, (requestOffset + requestLength): 13, allocSize: 8", e.getMessage());
        }
    }

    @Test
    void shouldAssertForInvalidOffsetForLong()
    {
        final int sizeInBytes = Long.BYTES;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(sizeInBytes);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, sizeInBytes);

        try
        {
            memory.putLong(-1, 123L);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 8, (requestOffset + requestLength): 7, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.getLong(-1);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 8, (requestOffset + requestLength): 7, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.putLong(Long.BYTES + 1, 123L);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 9, requestLength: 8, (requestOffset + requestLength): 17, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.getLong(Long.BYTES + 1);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 9, requestLength: 8, (requestOffset + requestLength): 17, allocSize: 8", e.getMessage());
        }
    }

    @Test
    void shouldAssertForInvalidOffsetForBytes()
    {
        final int sizeInBytes = Long.BYTES;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(sizeInBytes);
        final long baseAddress = NativeMemoryAccess.getBaseAddressForDirectBuffer(buffer);
        final Memory memory = new MemoryDirectImpl(baseAddress, sizeInBytes);

        final byte[] actual = new byte[Long.BYTES + 1];

        try
        {
            memory.putBytes(-1, actual);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 9, (requestOffset + requestLength): 8, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.putBytes(0, actual);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 0, requestLength: 9, (requestOffset + requestLength): 9, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.getBytes(0, actual);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: 0, requestLength: 9, (requestOffset + requestLength): 9, allocSize: 8", e.getMessage());
        }

        try
        {
            memory.getBytes(-1, actual);
        }
        catch (final AssertionError e)
        {
            assertEquals("requestOffset: -1, requestLength: 9, (requestOffset + requestLength): 8, allocSize: 8", e.getMessage());
        }
    }
}