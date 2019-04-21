package org.borer.logdb.bit;

import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;

import static org.borer.logdb.bit.MemoryAccess.UNSAFE_COPY_THRESHOLD_BYTES;
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

    @Test
    void shouldCopyPartOfSameHeapBuffer()
    {
        final long expectedSecondValue = 14564523L;
        final long expectedFirstValue = -1;
        final Memory memory = MemoryFactory.allocateHeap(2 * Long.BYTES, MemoryOrder.nativeOrder);
        memory.putLong(expectedSecondValue);

        final int secondValueOffset = Long.BYTES;
        MemoryCopy.copy(memory, 0, memory, secondValueOffset, Long.BYTES);

        memory.putLong(0, expectedFirstValue);

        assertEquals(expectedFirstValue, memory.getLong(0));
        assertEquals(expectedSecondValue, memory.getLong(secondValueOffset));
    }

    @Test
    void shouldCopyPartOfSameDirectBuffer()
    {
        final long expectedFirstValue = -1;
        final long expectedSecondValue = 14564523L;
        final Memory memory = MemoryFactory.allocateDirect(2 * Long.BYTES, MemoryOrder.nativeOrder);
        memory.putLong(expectedSecondValue);

        final int secondValueOffset = Long.BYTES;
        MemoryCopy.copy(memory, 0, memory, secondValueOffset, Long.BYTES);

        memory.putLong(0, expectedFirstValue);

        assertEquals(expectedFirstValue, memory.getLong(0));
        assertEquals(expectedSecondValue, memory.getLong(secondValueOffset));
    }

    @Test
    void shouldCopyBigMemoryInChunks()
    {
        final long expectedFirstValue = -1;
        final long expectedSecondValue = 14564523L;
        final int capacity = 10 * UNSAFE_COPY_THRESHOLD_BYTES;
        final int secondValueOffset = capacity - Long.BYTES;

        final Memory sourceMemory = MemoryFactory.allocateDirect(capacity, MemoryOrder.nativeOrder);
        final Memory destinationMemory = MemoryFactory.allocateDirect(capacity, MemoryOrder.nativeOrder);
        sourceMemory.putLong(expectedFirstValue);
        sourceMemory.putLong(secondValueOffset, expectedSecondValue);

        MemoryCopy.copy(sourceMemory, destinationMemory);

        assertEquals(expectedFirstValue, destinationMemory.getLong(0));
        assertEquals(expectedSecondValue, destinationMemory.getLong(secondValueOffset));
    }
}