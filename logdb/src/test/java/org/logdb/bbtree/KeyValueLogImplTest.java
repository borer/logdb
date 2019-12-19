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

class KeyValueLogImplTest
{
    private static final int MAX_LOG_KEY_VALUES = 10;
    private KeyValueLogImpl keyValueLog;

    @BeforeEach
    void setUp()
    {
        final HeapMemory memory = MemoryFactory.allocateHeap(1024, TestUtils.BYTE_ORDER);

        keyValueLog = KeyValueLogImpl.create(memory.slice(512));
    }

    @Test
    void shouldStartWithEmptyLog()
    {
        assertEquals(0, keyValueLog.getNumberOfPairs());
        assertEquals(KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE, keyValueLog.getUsedSize());
    }

    @Test
    void shouldBeAbleToInsertAndRetrieveSinglePair()
    {
        final byte[] keyBytes = "key".getBytes();
        final byte[] valueBytes = "test".getBytes();
        keyValueLog.putKeyValue(0, keyBytes, valueBytes);

        assertArrayEquals(keyBytes, keyValueLog.getKeyBytesAtIndex(0));
        assertArrayEquals(valueBytes, keyValueLog.getValueBytesAtIndex(0));
    }

    @Test
    void shouldBeAbleToInsertAndRetrieveMultiplePair()
    {
        int usedBytes = KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE;
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            final int expectedNumberOfPairs = i + 1;
            final int totalCellsSize = expectedNumberOfPairs * KeyValueLogImpl.CELL_SIZE;
            usedBytes += pair.key.length + pair.value.length;
            final int expectedUsedSize = usedBytes + totalCellsSize;

            keyValueLog.putKeyValue(i, pair.key, pair.value);

            assertEquals(expectedUsedSize, keyValueLog.getUsedSize());
            assertEquals(expectedNumberOfPairs, keyValueLog.getNumberOfPairs());
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertArrayEquals(pair.key, keyValueLog.getKeyBytesAtIndex(i));
            assertArrayEquals(pair.value, keyValueLog.getValueBytesAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToRetrievePairsByKey()
    {
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueLog.putKeyValue(i, pair.key, pair.value);
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesInOrder()
    {
        int usedBytes = KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE;
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            usedBytes += pair.key.length + pair.value.length;

            keyValueLog.putKeyValue(i, pair.key, pair.value);
        }

        //remove some values
        final int entriesToRemove = 5;
        for (int i = 0; i < entriesToRemove; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            final int expectedEntriesLeft = MAX_LOG_KEY_VALUES - (i + 1);
            final int totalCellsSize = expectedEntriesLeft * KeyValueLogImpl.CELL_SIZE;
            usedBytes -= keyValueLog.getKeyBytesAtIndex(i).length + keyValueLog.getValueBytesAtIndex(i).length;
            final int expectedUsedSize = usedBytes + totalCellsSize;

            assertTrue(keyValueLog.removeLogBytes(pair.key));

            assertEquals(expectedUsedSize, keyValueLog.getUsedSize());
            assertEquals(expectedEntriesLeft, keyValueLog.getNumberOfPairs());
        }

        //check the rest of values are present
        for (int i = 0; i < MAX_LOG_KEY_VALUES - entriesToRemove; i++)
        {
            final int key = i + entriesToRemove;
            final Pair pair = generateKeyValuePair(key);

            assertArrayEquals(pair.key, keyValueLog.getKeyBytesAtIndex(i));
            assertArrayEquals(pair.value, keyValueLog.getValueBytesAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesOutOfOrder()
    {
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueLog.putKeyValue(i, pair.key, pair.value);
        }

        final Predicate<Integer> shouldRemove = index -> index % 2 == 0;

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            if (shouldRemove.test(i))
            {
                final Pair pair = generateKeyValuePair(i);
                assertTrue(keyValueLog.removeLogBytes(pair.key));
            }
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            if (shouldRemove.negate().test(i))
            {
                final Pair pair = generateKeyValuePair(i);

                assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
            }
        }
    }

    @Test
    void shouldBeAbleToRemoveLogEntriesInReverseOrder()
    {
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            keyValueLog.putKeyValue(i, pair.key, pair.value);
        }

        long usedBytes = keyValueLog.getUsedSize();
        
        for (int i = MAX_LOG_KEY_VALUES - 1; i >= 0; i--)
        {
            final Pair pair = generateKeyValuePair(i);
            assertTrue(keyValueLog.removeLogBytes(pair.key));

            usedBytes -= KeyValueLogImpl.CELL_SIZE + pair.key.length + pair.value.length;
            assertEquals(usedBytes, keyValueLog.getUsedSize());
        }

        assertEquals(0, keyValueLog.getNumberOfPairs());
        assertEquals(KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE, keyValueLog.getUsedSize());
    }

    @Test
    void shouldBeAbleToRemoveAllLogPairs()
    {
        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) -> keyValueLog.insertLog( pair.key, pair.value));

        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) -> keyValueLog.removeLogBytes(pair.key));

        assertEquals(0, keyValueLog.getNumberOfPairs());
        assertEquals(KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE, keyValueLog.getUsedSize());

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i);

            assertNull(keyValueLog.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToUpdateLogEntriesWithBiggerValues()
    {
        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) -> keyValueLog.insertLog( pair.key, pair.value));

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "n");
            keyValueLog.insertLog(pair.key, pair.value);
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "n");
            assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToUpdateLogEntriesWithSmallerValues()
    {
        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "test");
            keyValueLog.putKeyValue(i, pair.key, pair.value);
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "t");
            keyValueLog.insertLog(pair.key, pair.value);
        }

        for (int i = 0; i < MAX_LOG_KEY_VALUES; i++)
        {
            final Pair pair = generateKeyValuePair(i, "", "t");
            assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
        }
    }

    @Test
    void shouldBeAbleToSpillLog()
    {
        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) -> keyValueLog.insertLog( pair.key, pair.value));

        final long expectedUsedSize = keyValueLog.getUsedSize();
        final KeyValueLogImpl spillLog = this.keyValueLog.spillLog();

        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) ->
        {
            assertArrayEquals(pair.key, spillLog.getKeyBytesAtIndex(index));
            assertArrayEquals(pair.value, spillLog.getValueBytesAtIndex(index));
        });

        assertEquals(MAX_LOG_KEY_VALUES, spillLog.getNumberOfPairs());
        assertEquals(expectedUsedSize, spillLog.getUsedSize());

        assertEquals(0, keyValueLog.getNumberOfPairs());
        assertEquals(KeyValueLogImpl.NUMBER_LOG_ENTRIES_SIZE, keyValueLog.getUsedSize());
    }

    @Test
    void shouldSplitLogWithEvenEntries()
    {
        final HeapMemory memory = MemoryFactory.allocateHeap(1024, TestUtils.BYTE_ORDER);
        final KeyValueLogImpl splitLog = KeyValueLogImpl.create(memory.slice(512));

        generateKeyValuePairs(MAX_LOG_KEY_VALUES, (index, pair) -> keyValueLog.insertLog( pair.key, pair.value));
        final int splitIndex = 5;
        final int expectedPairsInOriginal = 6;
        final int expectedPairsInSplit = 4;
        final byte[] splitKey = keyValueLog.getKeyBytesAtIndex(splitIndex);
        keyValueLog.splitLog(splitKey, splitLog);

        assertEquals(expectedPairsInOriginal, keyValueLog.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            final Pair pair = generateKeyValuePair(i);
            assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
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
        final KeyValueLogImpl splitLog = KeyValueLogImpl.create(memory.slice(512));

        final List<Pair> pairsOriginal = new ArrayList<>();
        final List<Pair> pairsSplit = new ArrayList<>();

        generateKeyValuePairs(11, (index, pair) -> keyValueLog.insertLog(pair.key, pair.value));

        final int splitIndex = 5;
        final int expectedPairsInOriginal = 6;
        final int expectedPairsInSplit = 5;
        final byte[] splitKey = keyValueLog.getKeyBytesAtIndex(splitIndex);

        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            pairsOriginal.add(new Pair(keyValueLog.getKeyBytesAtIndex(i), keyValueLog.getValueBytesAtIndex(i)));
        }
        for (int i = 0; i < expectedPairsInSplit; i++)
        {
            final int index = expectedPairsInOriginal + i;
            pairsSplit.add(new Pair(keyValueLog.getKeyBytesAtIndex(index), keyValueLog.getValueBytesAtIndex(index)));
        }

        keyValueLog.splitLog(splitKey, splitLog);


        assertEquals(expectedPairsInOriginal, keyValueLog.getNumberOfPairs());
        for (int i = 0; i < expectedPairsInOriginal; i++)
        {
            final Pair pair = pairsOriginal.get(i);
            assertArrayEquals(pair.value, keyValueLog.getValue(pair.key));
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