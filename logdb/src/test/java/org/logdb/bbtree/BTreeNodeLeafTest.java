package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.support.KeyValueUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.support.TestUtils.BYTE_ORDER;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class BTreeNodeLeafTest
{
    private BTreeNodeLeaf bTreeLeaf;
    private IdSupplier idSupplier;

    @BeforeEach
    void setUp()
    {
        idSupplier = new IdSupplier(0);

        final HeapMemory memory = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        bTreeLeaf = new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0);
    }

    /////////////////////////////////Add/Update

    @Test
    void shouldBeAbleToInsertNewValue()
    {
        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();
        bTreeLeaf.insert(key, value);

        assertArrayEquals(key, bTreeLeaf.getKeyBytes(0));
        assertArrayEquals(value, bTreeLeaf.getValueBytes(0));
        assertEquals(1, bTreeLeaf.getPairCount());
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValues()
    {
        int usedBytes = 0;
        for (int i = 0; i < 25; i++)
        {
            final KeyValueUtils.Pair pair = KeyValueUtils.generateKeyValuePair(i);
            bTreeLeaf.insert(pair.key, pair.value);

            usedBytes += pair.key.length + pair.value.length + BTreeNodePage.CELL_SIZE;

            assertArrayEquals(pair.value, bTreeLeaf.get(pair.key));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));
        }
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValuesInMemOrder()
    {
        final int count = 10;

        for (long i = count - 1; i >= 0; i--)
        {
            bTreeLeaf.insert(i, i);
        }

        assertEquals(count, bTreeLeaf.getPairCount());

        for (int i = 0; i < count; i++)
        {
            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
        }
    }

    @Test
    void shouldBeAbleToUpdateValuesWithExistingKey()
    {
        final long key = 99L;
        for (long i = 0; i < 10; i++)
        {
            bTreeLeaf.insert(key, i);

            assertEquals(key, bTreeLeaf.getKey(0));
            assertEquals(i, bTreeLeaf.getValue(0));
            assertEquals(1, bTreeLeaf.getPairCount());
        }
    }

    /////////////////////////////////Remove

    @Test
    void shouldBeAbleToRemoveEntry()
    {
        final long key = 99L;
        bTreeLeaf.insert(key, key);

        assertEquals(1, bTreeLeaf.getPairCount());

        try
        {
            bTreeLeaf.remove(0);
            assertEquals(0, bTreeLeaf.getPairCount());
        }
        catch (final AssertionError e)
        {
            assertEquals(0, bTreeLeaf.getPairCount());
            assertEquals("removing index 0 when key count is 0", e.getMessage());
        }
    }

    @Test
    void shouldBeAbleToRemoveEntriesFromFirstIndex()
    {
        int usedBytes = 0;
        final int numberOfElements = 10;
        for (int i = 0; i < numberOfElements; i++)
        {
            bTreeLeaf.insert(i, i);
            usedBytes += (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;

            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.remove(0);
            usedBytes -= (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
            assertEquals(i, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));

            for (int j = 0; j < bTreeLeaf.getPairCount(); j++)
            {
                final long expectedKey = numberOfElements - i + j;
                assertEquals(expectedKey, bTreeLeaf.getKey(j));
                assertEquals(expectedKey, bTreeLeaf.getValue(j));
            }
        }
    }

    @Test
    void shouldBeAbleToRemoveEntriesFromLastIndex()
    {
        final int numberOfElements = 10;
        for (int i = 0; i < numberOfElements; i++)
        {
            bTreeLeaf.insert(i, i);

            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.remove(i);
            assertEquals(i, bTreeLeaf.getPairCount());

            for (int j = 0; j < bTreeLeaf.getPairCount(); j++)
            {
                assertEquals(j, bTreeLeaf.getKey(j));
                assertEquals(j, bTreeLeaf.getValue(j));
            }
        }
    }

    @Test
    void shouldIgnoreRemovingNonExistentEntry()
    {
        try
        {
            bTreeLeaf.remove(0);
        }
        catch (final AssertionError e)
        {
            assertEquals(0, bTreeLeaf.getPairCount());
            assertEquals("removing index 0 when key count is 0", e.getMessage());
        }
    }

    /////////////////////////////////Get

    @Test
    void shouldBeAbleToGetValueByKey()
    {
        final long key = 99L;
        bTreeLeaf.insert(key, key);

        final long valueFound = bTreeLeaf.get(key);

        assertEquals(key, valueFound);
    }

    @Test
    void shouldGetNullForKeyNotFound()
    {
        final long valueFound = bTreeLeaf.get(10L);

        assertEquals(InvalidBTreeValues.KEY_NOT_FOUND_VALUE, valueFound);
    }

    /////////////////////////////////Split

    @Test
    void shouldBeAbleToSplitALeafWithEvenNumberOfElements()
    {
        final int totalElements = 10;
        for (int i = 0; i < totalElements; i++)
        {
            bTreeLeaf.insert(i, i);
        }

        assertEquals(totalElements, bTreeLeaf.getPairCount());

        final int at = totalElements >> 1;

        final HeapMemory memory = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf newBtree = new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0);
        bTreeLeaf.split(at, newBtree);

        assertEquals(at, bTreeLeaf.getPairCount());
        for (int i = 0; i < at; i++)
        {
            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
        }

        assertEquals(at, newBtree.getPairCount());
        for (int i = 0; i < at; i++)
        {
            final int key = i + at;
            final int value = i + at;
            assertEquals(key, newBtree.getKey(i));
            assertEquals(value, newBtree.getValue(i));
        }
    }

    @Test
    void shouldBeAbleToSplitALeafWithOddNumberOfElements()
    {
        final int totalElements = 11;
        final int at = totalElements >> 1;
        for (int i = 0; i < totalElements; i++)
        {
            bTreeLeaf.insert(i, i);
        }

        assertEquals(totalElements, bTreeLeaf.getPairCount());

        final HeapMemory memory = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf newBtree = new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0);
        bTreeLeaf.split(at, newBtree);

        assertEquals(at, bTreeLeaf.getPairCount());
        for (int i = 0; i < at; i++)
        {
            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
        }

        final int newBtreeSize = at + 1;
        assertEquals(newBtreeSize, newBtree.getPairCount());
        for (int i = 0; i < newBtreeSize; i++)
        {
            final int key = i + at;
            final int value = i + at;
            assertEquals(key, newBtree.getKey(i));
            assertEquals(value, newBtree.getValue(i));
        }
    }

    @Test
    void shouldDeepCopyLeafNode()
    {
        for (int i = 0; i < 10; i++)
        {
            bTreeLeaf.insert(i, i);
        }

        final BTreeNodeLeaf copy = new BTreeNodeLeaf(
                bTreeLeaf.getPageNumber(),
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0
        );
        bTreeLeaf.copy(copy);

        for (int i = 0; i < 10; i++)
        {
            bTreeLeaf.insert(i, 99L);
        }

        for (int i = 0; i < 10; i++)
        {
            assertEquals(i, copy.getKey(i));
            assertEquals(i, copy.getValue(i));
        }
    }
}