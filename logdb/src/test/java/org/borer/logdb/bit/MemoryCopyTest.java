package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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

    @Test
    void shouldNotBeAbleToCopyBetweenNativeAndNonNativeMemories()
    {
        final Memory memorySource = MemoryFactory.allocateDirect(Long.BYTES, MemoryOrder.nativeOrder);
        final Memory memoryDestination = MemoryFactory.allocateDirect(Long.BYTES, MemoryOrder.nonNativeOrder);

        final long expectedValue = 14564523L;
        memorySource.putLong(expectedValue);
        memoryDestination.putLong(expectedValue * 2);

        try
        {
            MemoryCopy.copy(memorySource, memoryDestination);
            fail();
        }
        catch (final RuntimeException e)
        {
            assertEquals("Copying between non-native and native memory order is not allowed.", e.getMessage());
        }
    }
}