package org.logdb.bit;

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

    @Test
    void shouldPutAndGetBytesIntoHeapAllocatedMemoryWithOffset()
    {
        final String expectedMsg = "this is a test";
        final byte[] expected =  expectedMsg.getBytes();
        final byte[] actual = new byte[expected.length];
        final int offset = 100;
        final ByteBuffer buffer = ByteBuffer.allocate(expected.length + offset);
        buffer.position(offset);
        final ByteBuffer sliceBuffer = buffer.slice();
        final Memory memory = new MemoryByteBufferImpl(sliceBuffer);

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
    void shouldCreateSliceMemoryFromOffset()
    {
        final int originalSize = 128;
        final int halfSize = originalSize / 2;
        final ByteBuffer buffer = ByteBuffer.allocate(originalSize);
        final Memory memory = new MemoryByteBufferImpl(buffer);
        final long expectedZeroPosition = 123456L;
        final long expectedHalfPosition = 987654L;

        memory.putLong(0, expectedZeroPosition);
        memory.putLong(halfSize, expectedHalfPosition);
        final Memory slice = memory.slice(halfSize);

        assertEquals(expectedZeroPosition, memory.getLong(0));
        assertEquals(expectedHalfPosition, memory.getLong(halfSize));
        assertEquals(originalSize, memory.getCapacity());

        assertEquals(halfSize, slice.getCapacity());
        assertEquals(expectedHalfPosition, slice.getLong(0));
    }

    @Test
    void shouldCreateSliceMemoryUpperHalf()
    {
        final int originalSize = 128;
        final int halfSize = originalSize / 2;
        final ByteBuffer buffer = ByteBuffer.allocate(originalSize);
        final Memory memory = new MemoryByteBufferImpl(buffer);
        final long expectedZeroPosition = 123456L;
        final long expectedHalfPosition = 987654L;

        memory.putLong(0, expectedZeroPosition);
        memory.putLong(halfSize, expectedHalfPosition);
        final Memory sliceUpperHalf = memory.sliceRange(halfSize, originalSize);

        assertEquals(expectedZeroPosition, memory.getLong(0));
        assertEquals(expectedHalfPosition, memory.getLong(halfSize));
        assertEquals(originalSize, memory.getCapacity());

        assertEquals(halfSize, sliceUpperHalf.getCapacity());
        assertEquals(expectedHalfPosition, sliceUpperHalf.getLong(0));
    }

    @Test
    void shouldCreateSliceMemoryBottomHalf()
    {
        final int originalSize = 128;
        final int halfSize = originalSize / 2;
        final ByteBuffer buffer = ByteBuffer.allocate(originalSize);
        final Memory memory = new MemoryByteBufferImpl(buffer);
        final long expectedZeroPosition = 123456L;
        final long expectedHalfPosition = 987654L;

        memory.putLong(0, expectedZeroPosition);
        memory.putLong(halfSize, expectedHalfPosition);
        final Memory sliceUpperHalf = memory.sliceRange(0, halfSize);

        assertEquals(expectedZeroPosition, memory.getLong(0));
        assertEquals(expectedHalfPosition, memory.getLong(halfSize));
        assertEquals(originalSize, memory.getCapacity());

        assertEquals(halfSize, sliceUpperHalf.getCapacity());
        assertEquals(expectedZeroPosition, sliceUpperHalf.getLong(0));
    }
}