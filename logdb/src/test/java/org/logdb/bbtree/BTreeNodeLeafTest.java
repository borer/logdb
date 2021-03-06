package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.BinaryHelper;
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

        assertArrayEquals(key, bTreeLeaf.getKey(0));
        assertArrayEquals(value, bTreeLeaf.getValue(0));
        assertEquals(1, bTreeLeaf.getPairCount());
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValues()
    {
        int usedBytes = BTreeNodePage.PAGE_HEADER_SIZE + KeyValueHeapImpl.HEADER_SIZE;
        for (int i = 0; i < 25; i++)
        {
            final KeyValueUtils.Pair pair = KeyValueUtils.generateKeyValuePair(i);
            bTreeLeaf.insert(pair.key, pair.value);

            usedBytes += pair.key.length + pair.value.length + BTreeNodePage.CELL_SIZE;

            assertArrayEquals(pair.value, bTreeLeaf.get(pair.key));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));
        }
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValuesInMemOrder()
    {
        final int count = 10;

        for (long i = count - 1; i >= 0; i--)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);
        }

        assertEquals(count, bTreeLeaf.getPairCount());

        for (int i = 0; i < count; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(bytes, bTreeLeaf.getKey(i));
            assertArrayEquals(bytes, bTreeLeaf.getValue(i));
        }
    }

    @Test
    void shouldBeAbleToUpdateValuesWithExistingKey()
    {
        final byte[] key = BinaryHelper.longToBytes(99L);
        for (long i = 0; i < 10; i++)
        {
            final byte[] value = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(key, value);

            assertArrayEquals(key, bTreeLeaf.getKey(0));
            assertArrayEquals(value, bTreeLeaf.getValue(0));
            assertEquals(1, bTreeLeaf.getPairCount());
        }
    }

    /////////////////////////////////Remove

    @Test
    void shouldBeAbleToRemoveEntry()
    {
        final byte[] key = BinaryHelper.longToBytes(99L);
        bTreeLeaf.insert(key, key);

        assertEquals(1, bTreeLeaf.getPairCount());

        try
        {
            bTreeLeaf.removeAtIndex(0);
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
        int usedBytes = BTreeNodePage.PAGE_HEADER_SIZE + KeyValueHeapImpl.HEADER_SIZE;
        final int numberOfElements = 10;
        for (int i = 0; i < numberOfElements; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);
            usedBytes += (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;

            assertArrayEquals(bytes, bTreeLeaf.getKey(i));
            assertArrayEquals(bytes, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.removeAtIndex(0);
            usedBytes -= (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
            assertEquals(i, bTreeLeaf.getPairCount());
            assertEquals(PAGE_SIZE_BYTES - usedBytes, bTreeLeaf.calculateFreeSpaceLeft(PAGE_SIZE_BYTES));

            for (int j = 0; j < bTreeLeaf.getPairCount(); j++)
            {
                final byte[] expectedKey = BinaryHelper.longToBytes(numberOfElements - i + j);
                assertArrayEquals(expectedKey, bTreeLeaf.getKey(j));
                assertArrayEquals(expectedKey, bTreeLeaf.getValue(j));
            }
        }
    }

    @Test
    void shouldBeAbleToRemoveEntriesFromLastIndex()
    {
        final int numberOfElements = 10;
        for (int i = 0; i < numberOfElements; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);

            assertArrayEquals(bytes, bTreeLeaf.getKey(i));
            assertArrayEquals(bytes, bTreeLeaf.getValue(i));
            assertEquals(i + 1, bTreeLeaf.getPairCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.removeAtIndex(i);
            assertEquals(i, bTreeLeaf.getPairCount());

            for (int j = 0; j < bTreeLeaf.getPairCount(); j++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(j);
                assertArrayEquals(bytes, bTreeLeaf.getKey(j));
                assertArrayEquals(bytes, bTreeLeaf.getValue(j));
            }
        }
    }

    @Test
    void shouldIgnoreRemovingNonExistentEntry()
    {
        try
        {
            bTreeLeaf.removeAtIndex(0);
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
        final byte[] key = BinaryHelper.longToBytes(99L);
        bTreeLeaf.insert(key, key);

        final byte[] valueFound = bTreeLeaf.get(key);

        assertArrayEquals(key, valueFound);
    }

    @Test
    void shouldGetNullForKeyNotFound()
    {
        final byte[] valueFound = bTreeLeaf.get(BinaryHelper.longToBytes(10L));

        assertArrayEquals(null, valueFound);
    }

    /////////////////////////////////Split

    @Test
    void shouldBeAbleToSplitALeafWithEvenNumberOfElements()
    {
        final int totalElements = 10;
        for (int i = 0; i < totalElements; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);
        }

        assertEquals(totalElements, bTreeLeaf.getPairCount());

        final int at = totalElements >> 1;

        final HeapMemory memory = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf newBtree = new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0);
        bTreeLeaf.split(at, newBtree);

        assertEquals(at, bTreeLeaf.getPairCount());
        for (int i = 0; i < at; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);

            assertArrayEquals(bytes, bTreeLeaf.getKey(i));
            assertArrayEquals(bytes, bTreeLeaf.getValue(i));
        }

        assertEquals(at, newBtree.getPairCount());
        for (int i = 0; i < at; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i + at);
            final byte[] value = BinaryHelper.longToBytes(i + at);
            assertArrayEquals(key, newBtree.getKey(i));
            assertArrayEquals(value, newBtree.getValue(i));
        }
    }

    @Test
    void shouldBeAbleToSplitALeafWithOddNumberOfElements()
    {
        final int totalElements = 11;
        final int at = totalElements >> 1;
        for (int i = 0; i < totalElements; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);
        }

        assertEquals(totalElements, bTreeLeaf.getPairCount());

        final HeapMemory memory = MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER);
        final BTreeNodeLeaf newBtree = new BTreeNodeLeaf(idSupplier.getAsLong(), memory, 0);
        bTreeLeaf.split(at, newBtree);

        assertEquals(at, bTreeLeaf.getPairCount());
        for (int i = 0; i < at; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);

            assertArrayEquals(bytes, bTreeLeaf.getKey(i));
            assertArrayEquals(bytes, bTreeLeaf.getValue(i));
        }

        final int newBtreeSize = at + 1;
        assertEquals(newBtreeSize, newBtree.getPairCount());
        for (int i = 0; i < newBtreeSize; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i + at);
            final byte[] value = BinaryHelper.longToBytes(i + at);
            assertArrayEquals(key, newBtree.getKey(i));
            assertArrayEquals(value, newBtree.getValue(i));
        }
    }

    @Test
    void shouldDeepCopyLeafNode()
    {
        for (int i = 0; i < 10; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeLeaf.insert(bytes, bytes);
        }

        final BTreeNodeLeaf copy = new BTreeNodeLeaf(
                bTreeLeaf.getPageNumber(),
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0
        );
        bTreeLeaf.copy(copy);

        for (int i = 0; i < 10; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);

            bTreeLeaf.insert(bytes, BinaryHelper.longToBytes(99L));
        }

        for (int i = 0; i < 10; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);

            assertArrayEquals(bytes, copy.getKey(i));
            assertArrayEquals(bytes, copy.getValue(i));
        }
    }
}