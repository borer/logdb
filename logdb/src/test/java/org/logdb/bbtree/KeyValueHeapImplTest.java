package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.support.KeyValueUtils.Pair;
import org.logdb.support.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.logdb.support.KeyValueUtils.generateKeyValuePair;
import static org.logdb.support.KeyValueUtils.generateKeyValuePairs;

class KeyValueHeapImplTest
{
    private static final int MAX_KEY_VALUES = 10;
    private KeyValueHeapImpl keyValueHeap;

    @BeforeEach
    void setUp()
    {
        final HeapMemory memory = MemoryFactory.allocateHeap(1024, TestUtils.BYTE_ORDER);

        keyValueHeap = KeyValueHeapImpl.create(memory.slice(512));
    }

    @Test
    void shouldStartWithEmpty()
    {
        assertEquals(0, keyValueHeap.getNumberOfPairs());
        assertEquals(KeyValueHeapImpl.HEADER_SIZE, keyValueHeap.getUsedSize());
    }

    @Test
    void shouldBeAbleToInsertAndRetrieveSinglePair()
    {
        final byte[] keyBytes = "key".getBytes();
        final byte[] valueBytes = "test".getBytes();
        keyValueHeap.insertAtIndex(0, keyBytes, valueBytes);

        assertArrayEquals(keyBytes, keyValueHeap.getKeyAtIndex(0));
        assertArrayEquals(valueBytes, keyValueHeap.getValueAtIndex(0));
    }

    @Test
    void shouldBeAbleToInsertAndRetrieveMultiplePair()
    {
        int usedBytes = KeyValueHeapImpl.HEADER_SIZE;
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            final int expectedNumberOfPairs = i + 1;
            final int totalCellsSize = expectedNumberOfPairs * KeyValueHeapImpl.CELL_SIZE;
            usedBytes += pair.key.length + pair.value.length;
            final int expectedUsedSize = usedBytes + totalCellsSize;

            keyValueHeap.insertAtIndex(i, pair.key, pair.value);

            assertEquals(expectedUsedSize, keyValueHeap.getUsedSize());
            assertEquals(expectedNumberOfPairs, keyValueHeap.getNumberOfPairs());
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertArrayEquals(pair.key, keyValueHeap.getKeyAtIndex(i));
            assertArrayEquals(pair.value, keyValueHeap.getValueAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToRetrievePairsByKey()
    {
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueHeap.insertAtIndex(i, pair.key, pair.value);
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesInOrder()
    {
        int usedBytes = KeyValueHeapImpl.HEADER_SIZE;
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            usedBytes += pair.key.length + pair.value.length;

            keyValueHeap.insertAtIndex(i, pair.key, pair.value);
        }

        //remove some values
        final int entriesToRemove = 5;
        for (int i = 0; i < entriesToRemove; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            final int expectedEntriesLeft = MAX_KEY_VALUES - (i + 1);
            final int totalCellsSize = expectedEntriesLeft * KeyValueHeapImpl.CELL_SIZE;
            usedBytes -= keyValueHeap.getKeyAtIndex(i).length + keyValueHeap.getValueAtIndex(i).length;
            final int expectedUsedSize = usedBytes + totalCellsSize;

            assertTrue(keyValueHeap.removeKeyValue(pair.key));

            assertEquals(expectedUsedSize, keyValueHeap.getUsedSize());
            assertEquals(expectedEntriesLeft, keyValueHeap.getNumberOfPairs());
        }

        //check the rest of values are present
        for (int i = 0; i < MAX_KEY_VALUES - entriesToRemove; i++)
        {
            final int key = i + entriesToRemove;
            final Pair pair = generateKeyValuePair(key);

            assertArrayEquals(pair.key, keyValueHeap.getKeyAtIndex(i));
            assertArrayEquals(pair.value, keyValueHeap.getValueAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesOutOfOrder()
    {
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueHeap.insertAtIndex(i, pair.key, pair.value);
        }

        final Predicate<Integer> shouldRemove = index -> index % 2 == 0;

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            if (shouldRemove.test(i))
            {
                final Pair pair = generateKeyValuePair(i);
                assertTrue(keyValueHeap.removeKeyValue(pair.key));
            }
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            if (shouldRemove.negate().test(i))
            {
                final Pair pair = generateKeyValuePair(i);

                assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
            }
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesInReverseOrder()
    {
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueHeap.insertAtIndex(i, pair.key, pair.value);
        }

        long usedBytes = keyValueHeap.getUsedSize();
        
        for (int i = MAX_KEY_VALUES - 1; i >= 0; i--)
        {
            final Pair pair = generateKeyValuePair(i);
            assertTrue(keyValueHeap.removeKeyValue(pair.key));

            usedBytes -= KeyValueHeapImpl.CELL_SIZE + pair.key.length + pair.value.length;
            assertEquals(usedBytes, keyValueHeap.getUsedSize());
        }

        assertEquals(0, keyValueHeap.getNumberOfPairs());
        assertEquals(KeyValueHeapImpl.HEADER_SIZE, keyValueHeap.getUsedSize());
    }

    @Test
    void shouldBeAbleToRemoveAllLogPairs()
    {
        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) -> keyValueHeap.insert( pair.key, pair.value));

        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) -> keyValueHeap.removeKeyValue(pair.key));

        assertEquals(0, keyValueHeap.getNumberOfPairs());
        assertEquals(KeyValueHeapImpl.HEADER_SIZE, keyValueHeap.getUsedSize());

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertNull(keyValueHeap.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToUpdateEntriesWithSameLengthValues()
    {
        final byte[] key = "testKey".getBytes();
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueHeap.insert(key, pair.value);

            assertArrayEquals(pair.value, keyValueHeap.getValue(key));
        }
    }

    @Test
    void shouldBeAbleToUpdateEntriesWithBiggerValues()
    {
        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) -> keyValueHeap.insert( pair.key, pair.value));

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "n");
            keyValueHeap.insert(pair.key, pair.value);
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "n");
            assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToUpdateEntriesWithSmallerValues()
    {
        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "test");
            keyValueHeap.insertAtIndex(i, pair.key, pair.value);
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "t");
            keyValueHeap.insert(pair.key, pair.value);
        }

        for (int i = 0; i < MAX_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "t");
            assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToSpillLog()
    {
        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) -> keyValueHeap.insert( pair.key, pair.value));

        final long expectedUsedSize = keyValueHeap.getUsedSize();
        final KeyValueHeapImpl spillLog = this.keyValueHeap.spill();

        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) ->
        {
            assertArrayEquals(pair.key, spillLog.getKeyAtIndex(index));
            assertArrayEquals(pair.value, spillLog.getValueAtIndex(index));
        });

        assertEquals(MAX_KEY_VALUES, spillLog.getNumberOfPairs());
        assertEquals(expectedUsedSize, spillLog.getUsedSize());

        assertEquals(0, keyValueHeap.getNumberOfPairs());
        assertEquals(KeyValueHeapImpl.HEADER_SIZE, keyValueHeap.getUsedSize());
    }

    @Test
    void shouldSplitLogWithEvenEntries()
    {
        final HeapMemory memory = MemoryFactory.allocateHeap(1024, TestUtils.BYTE_ORDER);
        final KeyValueHeapImpl splitLog = KeyValueHeapImpl.create(memory.slice(512));

        generateKeyValuePairs(MAX_KEY_VALUES, (index, pair) -> keyValueHeap.insert( pair.key, pair.value));
        final int splitIndex = 5;
        final int expectedPairsInOriginal = 6;
        final int expectedPairsInSplit = 4;
        final byte[] splitKey = keyValueHeap.getKeyAtIndex(splitIndex);
        keyValueHeap.split(splitKey, splitLog);

        assertEquals(expectedPairsInOriginal, keyValueHeap.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
        }

        assertEquals(expectedPairsInSplit, splitLog.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInSplit; i++)
        {
            final int index = i + expectedPairsInOriginal;
            final Pair pair = generateKeyValuePair(index);

            assertArrayEquals(pair.value, splitLog.getValue(pair.key));
        }
    }

    @Test
    void shouldSplitLogWithOddEntries()
    {
        final HeapMemory memory = MemoryFactory.allocateHeap(1024, TestUtils.BYTE_ORDER);
        final KeyValueHeapImpl splitLog = KeyValueHeapImpl.create(memory.slice(512));

        final List<Pair> pairsOriginal = new ArrayList<>();
        final List<Pair> pairsSplit = new ArrayList<>();

        generateKeyValuePairs(11, (index, pair) -> keyValueHeap.insert(pair.key, pair.value));

        final int splitIndex = 5;
        final int expectedPairsInOriginal = 6;
        final int expectedPairsInSplit = 5;
        final byte[] splitKey = keyValueHeap.getKeyAtIndex(splitIndex);

        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            pairsOriginal.add(new Pair(keyValueHeap.getKeyAtIndex(i), keyValueHeap.getValueAtIndex(i)));
        }
        for (int i = 0; i < expectedPairsInSplit; i++)
        {
            final int index = expectedPairsInOriginal + i;
            pairsSplit.add(new Pair(keyValueHeap.getKeyAtIndex(index), keyValueHeap.getValueAtIndex(index)));
        }

        keyValueHeap.split(splitKey, splitLog);


        assertEquals(expectedPairsInOriginal, keyValueHeap.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            final Pair pair = pairsOriginal.get(i);
            assertArrayEquals(pair.value, keyValueHeap.getValue(pair.key));
        }

        assertEquals(expectedPairsInSplit, splitLog.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInSplit; i++)
        {
            final Pair pair = pairsSplit.get(i);
            assertArrayEquals(pair.value, splitLog.getValue(pair.key));
        }
    }

    //TODO: add a test when a keyValue is full/almost full and do operation on it, addition, removal, ge
}