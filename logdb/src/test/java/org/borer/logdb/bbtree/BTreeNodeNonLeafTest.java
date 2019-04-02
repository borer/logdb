package org.borer.logdb.bbtree;

import org.borer.logdb.bit.MemoryFactory;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.borer.logdb.Config.PAGE_SIZE_BYTES;
import static org.borer.logdb.support.TestUtils.BYTE_ORDER;
import static org.borer.logdb.support.TestUtils.createLeafNodeWithKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BTreeNodeNonLeafTest
{
    private BTreeNodeNonLeaf bTreeNonLeaf;

    @BeforeEach
    void setUp()
    {
        final BTreeNode bTreeNode = createLeafNodeWithKeys(10, 0, new IdSupplier(0));
        bTreeNonLeaf = TestUtils.createNonLeafNodeWithChild(bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertChild()
    {
        final BTreeNode bTreeNode = createLeafNodeWithKeys(10, 0, new IdSupplier(0));

        bTreeNonLeaf.insertChild(0, 0, bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }

        for (int i = 0; i < 11; i++)
        {
            final int expectedValue = (i == 10) ? 0 : numKeysPerChild * i; // the first value when crete bTreeNonLeaf is 0
            assertEquals(expectedValue, bTreeNonLeaf.getKey(i));
        }
    }

    @Test
    void shouldBeAbleToDeleteChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);
        }

        assertEquals(10, bTreeNonLeaf.getKeyCount());

        bTreeNonLeaf.remove(9); //remove last key item
        assertEquals(9, bTreeNonLeaf.getKeyCount());
        for (int i = 0; i < 9; i++)
        {
            final int expectedValue = numKeysPerChild * i;
            assertEquals(expectedValue, bTreeNonLeaf.getKey(i));
            assertEquals(BTreeNodeNonLeaf.NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(i));
        }

        bTreeNonLeaf.remove(2);
        assertEquals(8, bTreeNonLeaf.getKeyCount());
        for (int i = 0; i < 8; i++)
        {
            final int expectedValue = numKeysPerChild * ((i >= 2) ? i + 1 : i);
            assertEquals(expectedValue, bTreeNonLeaf.getKey(i));
        }
    }

    @Test
    void shouldIgnoreRemovingNonExistentChildren()
    {
        try
        {
            bTreeNonLeaf.remove(0);
        }
        catch (final AssertionError e)
        {
            assertEquals(0, bTreeNonLeaf.getKeyCount());
            assertEquals("removing index 0 when key count is 0", e.getMessage());
        }
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithEvenKeyNumber()
    {
        final int numKeysPerChild = 5;
        final int expectedKeysInCurrent = 5;
        final int expectedKeysInSplit = 4;
        final int totalKeys = 10;
        for (int i = 0; i < totalKeys; i++)
        {
            final long key = numKeysPerChild * i;
            final BTreeNode child = TestUtils.createLeafNodeWithKeys(0, i, new IdSupplier(i));

            bTreeNonLeaf.insertChild(i, key, child); //there is something funky with the byte order
        }

        final int at = totalKeys >> 1;
        final BTreeNodeNonLeaf split = new BTreeNodeNonLeaf(
                0L,
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                1, //there is always one child at least
                new BTreeNode[1]);
        bTreeNonLeaf.split(at, split);

        assertEquals(expectedKeysInCurrent, bTreeNonLeaf.getKeyCount());
        assertEquals(expectedKeysInSplit, split.getKeyCount());

        final long[] currentKeys = new long[expectedKeysInCurrent];
        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final long expectedKey = numKeysPerChild * i;
            final long keyAtIndex = bTreeNonLeaf.getKey(i);
            assertEquals(expectedKey, keyAtIndex);

            currentKeys[i] = keyAtIndex;
        }

        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final long currentKey = currentKeys[i];
            for (int j = 0; j < expectedKeysInSplit; j++)
            {
                assertNotEquals(currentKey, split.getKey(j));
            }
        }

        final long[] splitKeys = new long[expectedKeysInCurrent];
        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final int keyOffsetDueToLostKey = i + 1;
            final int keyOffsetDueToSplit = keyOffsetDueToLostKey + expectedKeysInCurrent;
            final long expectedKey = numKeysPerChild * keyOffsetDueToSplit;
            final long keyAtIndex = split.getKey(i);
            assertEquals(expectedKey, keyAtIndex);

            splitKeys[i] = keyAtIndex;
        }

        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final long splitKey = splitKeys[i];
            for (int j = 0; j < expectedKeysInCurrent; j++)
            {
                assertNotEquals(splitKey, bTreeNonLeaf.getKey(j));
            }
        }
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithOddKeyNumber()
    {
        final int numKeysPerChild = 5;
        final int expectedKeysInCurrent = 5;
        final int expectedKeysInSplit = 5;
        final int totalKeys = 11;
        for (int i = 0; i < totalKeys; i++)
        {
            final long key = numKeysPerChild * i;
            final BTreeNode child = TestUtils.createLeafNodeWithKeys(0, i, new IdSupplier(i));

            bTreeNonLeaf.insertChild(i, key, child);//there is something funky with the byte order
        }

        final int at = totalKeys >> 1;
        final BTreeNodeNonLeaf split = new BTreeNodeNonLeaf(
                0L,
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                1, //there is always one child at least
                new BTreeNode[1]);
        bTreeNonLeaf.split(at, split);

        assertEquals(expectedKeysInCurrent, bTreeNonLeaf.getKeyCount());
        assertEquals(expectedKeysInSplit, split.getKeyCount());
        final long[] currentKeys = new long[expectedKeysInCurrent];

        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final long expectedKey = numKeysPerChild * i;
            final long keyAtIndex = bTreeNonLeaf.getKey(i);
            assertEquals(expectedKey, keyAtIndex);

            currentKeys[i] = keyAtIndex;
        }

        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            assertNotEquals(currentKeys[i], split.getKey(i));
        }

        final long[] splitKeys = new long[expectedKeysInCurrent];
        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final int expectedKeyIndex = (i + 1) + expectedKeysInCurrent; //we lose one key when split
            final long expectedKey = numKeysPerChild * expectedKeyIndex;
            final long keyAtIndex = split.getKey(i);
            assertEquals(expectedKey, keyAtIndex);

            splitKeys[i] = keyAtIndex;
        }

        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            assertNotEquals(splitKeys[i], bTreeNonLeaf.getKey(i));
        }
    }

    @Test
    void shouldDeepCopyNonLeafNode()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }

        final BTreeNodeNonLeaf copy = new BTreeNodeNonLeaf(
                bTreeNonLeaf.getPageNumber(),
                MemoryFactory.allocateHeap(PAGE_SIZE_BYTES, BYTE_ORDER),
                0,
                0, //there is always one child at least
                null);

        bTreeNonLeaf.copy(copy);

        final int key = numKeysPerChild * 10;
        final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));
        bTreeNonLeaf.insertChild(8, key, child);

        assertEquals(11, bTreeNonLeaf.getKeyCount());
        assertEquals(10, copy.getKeyCount());
    }
}