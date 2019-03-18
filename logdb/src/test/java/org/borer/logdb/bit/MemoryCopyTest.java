package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryCopyTest
{
    @Test
    void shouldBeAbleToCopyBetweenNativeDirectBuffers()
    {
        final Memory memorySource = MemoryFactory.allocateDirect(Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final Memory memoryDestination = MemoryFactory.allocateDirect(Long.BYTES, ByteOrder.LITTLE_ENDIAN);

        final long expectedValue = 123L;
        memorySource.putLong(expectedValue);
        memoryDestination.putLong(expectedValue * 2);

        MemoryCopy.copy(memorySource, memoryDestination);

        assertEquals(expectedValue, memoryDestination.getLong(0));
    }

    @Test
    void shouldBeAbleToCopyBetweenHeapAndNativeDirectBuffer()
    {
        final Memory memorySource = MemoryFactory.allocateHeap(Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final Memory memoryDestination = MemoryFactory.allocateDirect(Long.BYTES, ByteOrder.LITTLE_ENDIAN);

        final long expectedValue = 123L;
        memorySource.putLong(expectedValue);
        memoryDestination.putLong(expectedValue * 2);

        MemoryCopy.copy(memorySource, memoryDestination);

        assertEquals(expectedValue, memoryDestination.getLong(0));
    }

    @Test
    void shouldBeAbleToCopyBetweenNativeDirectBufferAndHeap()
    {
        final Memory memorySource = MemoryFactory.allocateDirect(Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final Memory memoryDestination = MemoryFactory.allocateHeap(Long.BYTES, ByteOrder.LITTLE_ENDIAN);

        final long expectedValue = 123L;
        memorySource.putLong(expectedValue);
        memoryDestination.putLong(expectedValue * 2);

        MemoryCopy.copy(memorySource, memoryDestination);

        assertEquals(expectedValue, memoryDestination.getLong(0));
    }

    @Test
    void shouldBeAbleToCopyBetweenHeapBuffers()
    {
        final Memory memorySource = MemoryFactory.allocateHeap(Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final Memory memoryDestination = MemoryFactory.allocateHeap(Long.BYTES, ByteOrder.LITTLE_ENDIAN);

        final long expectedValue = 123L;
        memorySource.putLong(expectedValue);
        memoryDestination.putLong(expectedValue * 2);

        MemoryCopy.copy(memorySource, memoryDestination);

        assertEquals(expectedValue, memoryDestination.getLong(0));
    }
}