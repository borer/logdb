package org.borer.logdb.bbtree;

import org.borer.logdb.bit.Memory;
import org.borer.logdb.bit.MemoryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.borer.logdb.Config.BYTE_ORDER;
import static org.borer.logdb.Config.PAGE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreeNodeLeafTest
{
    private BTreeNodeLeaf bTreeLeaf;

    @BeforeEach
    void setUp()
    {
        bTreeLeaf = new BTreeNodeLeaf(
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                0,
                new IdSupplier(0));
    }

    /////////////////////////////////Add/Update

    @Test
    void shouldBeAbleToInsertNewValue()
    {
        final int key = 1;
        bTreeLeaf.insert(key, key);

        assertEquals(key, bTreeLeaf.getKey(0));
        assertEquals(key, bTreeLeaf.getValue(0));
        assertEquals(1, bTreeLeaf.getKeyCount());
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValues()
    {
        for (int key = 10, i = 0; key < 100; i++,key+=10)
        {
            bTreeLeaf.insert(key, key);

            assertEquals(key, bTreeLeaf.getKey(i));
            assertEquals(key, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
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

        assertEquals(count, bTreeLeaf.getKeyCount());

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
            assertEquals(1, bTreeLeaf.getKeyCount());
        }
    }

    /////////////////////////////////Remove

    @Test
    void shouldBeAbleToRemoveEntry()
    {
        final long key = 99L;
        bTreeLeaf.insert(key, key);

        assertEquals(1, bTreeLeaf.getKeyCount());

        try
        {
            bTreeLeaf.remove(0);
        }
        catch (final AssertionError e)
        {
            assertEquals(0, bTreeLeaf.getKeyCount());
            assertEquals("removing index 0 when key count is 0", e.getMessage());
        }
    }

    @Test
    void shouldBeAbleToRemoveEntriesFromFirstIndex()
    {
        final int numberOfElements = 10;
        for (int i = 0; i < numberOfElements; i++)
        {
            bTreeLeaf.insert(i, i);

            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.remove(0);
            assertEquals(i, bTreeLeaf.getKeyCount());

            for (int j = 0; j < bTreeLeaf.getKeyCount(); j++)
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
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.remove(i);
            assertEquals(i, bTreeLeaf.getKeyCount());

            for (int j = 0; j < bTreeLeaf.getKeyCount(); j++)
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
            assertEquals(0, bTreeLeaf.getKeyCount());
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

        assertEquals(-1, valueFound);
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

        assertEquals(totalElements, bTreeLeaf.getKeyCount());

        final int at = totalElements >> 1;
        final BTreeNodeLeaf newBtree = (BTreeNodeLeaf)bTreeLeaf.split(at, MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER));

        assertEquals(at, bTreeLeaf.getKeyCount());
        for (int i = 0; i < at; i++)
        {
            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
        }

        assertEquals(at, newBtree.getKeyCount());
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

        assertEquals(totalElements, bTreeLeaf.getKeyCount());

        final BTreeNodeLeaf newBtree = (BTreeNodeLeaf)bTreeLeaf.split(at, MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER));

        assertEquals(at, bTreeLeaf.getKeyCount());
        for (int i = 0; i < at; i++)
        {
            assertEquals(i, bTreeLeaf.getKey(i));
            assertEquals(i, bTreeLeaf.getValue(i));
        }

        final int newBtreeSize = at + 1;
        assertEquals(newBtreeSize, newBtree.getKeyCount());
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

        final Memory memoryForCopy = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf copy = (BTreeNodeLeaf)bTreeLeaf.copy(memoryForCopy);

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