package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.BinaryHelper;
import org.logdb.bit.MemoryFactory;
import org.logdb.support.KeyValueUtils;
import org.logdb.support.TestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.logdb.bbtree.BTreeNodeNonLeaf.NON_COMMITTED_CHILD;
import static org.logdb.support.KeyValueUtils.generateKeyValuePair;
import static org.logdb.support.KeyValueUtils.generateKeyValuePairs;

class BTreeNodeNonLeafTest
{
    private static final int BIG_PAGE_SIZE_BYTES = 2 * TestUtils.PAGE_SIZE_BYTES;
    private static final int BIG_NODE_LOG_SIZE = 2 * TestUtils.NODE_LOG_SIZE;

    private BTreeNodeNonLeaf bTreeNonLeaf;

    @BeforeEach
    void setUp()
    {
        final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(10, 0, new IdSupplier(0));
        bTreeNonLeaf = TestUtils.createNonLeafNodeWithChild(bTreeNode, BIG_PAGE_SIZE_BYTES, BIG_NODE_LOG_SIZE);
    }

    @Test
    void shouldBeAbleToInsertChild()
    {
        final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(10, 0, new IdSupplier(0));

        bTreeNonLeaf.insertChild(0, BinaryHelper.longToBytes(0), bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertLogKeyValues()
    {
        final int maxLogKeyValues = 6;
        for (int i = 0; i < maxLogKeyValues; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTreeNonLeaf.insertLog(bytes, bytes);
        }

        for (int i = 0; i < maxLogKeyValues; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(bytes, bTreeNonLeaf.getLogValueAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToRemoveLogKeyValues()
    {
        final int maxLogKeyValues = 6;
        generateKeyValuePairs(maxLogKeyValues, (index, pair) -> bTreeNonLeaf.insertLog(pair.key, pair.value));

        generateKeyValuePairs(maxLogKeyValues, (index, pair) -> bTreeNonLeaf.removeLog(pair.key));

        for (int i = 0; i < maxLogKeyValues; i++)
        {
            assertEquals(-1, bTreeNonLeaf.binarySearchInLog(BinaryHelper.longToBytes(i)));
        }
    }

    @Test
    void shouldNotFailWhenRemovingNonExistingLogKeyValue()
    {
        assertEquals(0, bTreeNonLeaf.getNumberOfLogPairs());

        final int maxLogKeyValues = 10;
        for (int i = 0; i < maxLogKeyValues; i++)
        {
            bTreeNonLeaf.removeLog(BinaryHelper.longToBytes(i));
        }

        assertEquals(0, bTreeNonLeaf.getNumberOfLogPairs());
    }

    @Test
    void shouldTryToGetKeyForRightMostChild()
    {
        final int key = 5;
        final byte[] expectedInsertedChildKey = BinaryHelper.longToBytes(key);
        final byte[] expectedRightmostChildKey = new byte[0];

        final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(5, key, new IdSupplier(key));

        bTreeNonLeaf.insertChild(0, expectedInsertedChildKey, bTreeNode);

        assertArrayEquals(expectedInsertedChildKey, bTreeNonLeaf.getKey(0));
        assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(0));

        assertArrayEquals(expectedRightmostChildKey, bTreeNonLeaf.getKey(1));
        assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(1));
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        int usedBytes = Long.BYTES + BTreeNodePage.CELL_SIZE; // nonLeaf nodes always start with a long value and initial entry in the cell arrays.
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final byte[] keyBytes = BinaryHelper.longToBytes(key);
            final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            usedBytes += (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
            final int expectedFreeSize = BIG_PAGE_SIZE_BYTES - BIG_NODE_LOG_SIZE - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES;

            bTreeNonLeaf.insertChild(i, keyBytes, bTreeNode);

            final int expectedPairs = i + 2;// +1 to convert index to number of and + 1 to account to the rightmost pair
            assertEquals(expectedPairs, bTreeNonLeaf.getPairCount());
            assertEquals(expectedFreeSize, bTreeNonLeaf.calculateFreeSpaceLeft(BIG_PAGE_SIZE_BYTES));
        }

        for (int i = 0; i < 11; i++)
        {
            // the first value when crete bTreeNonLeaf is 0
            if (i == 10)
            {
                assertArrayEquals(new byte[0], bTreeNonLeaf.getKey(i));
            }
            else
            {
                final byte[] expectedValue = BinaryHelper.longToBytes(numKeysPerChild * i);
                assertArrayEquals(expectedValue, bTreeNonLeaf.getKey(i));
            }
        }
    }

    @Test
    void shouldBeAbleToDeleteChildren()
    {
        int usedBytes = Long.BYTES + BTreeNodePage.CELL_SIZE; // nonLeaf nodes always start with a long value and initial entry in the cell arrays.
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final byte[] keyBytes = BinaryHelper.longToBytes(key);
            final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            usedBytes += (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
            final int expectedFreeSize = BIG_PAGE_SIZE_BYTES - BIG_NODE_LOG_SIZE - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES;

            bTreeNonLeaf.insertChild(i, keyBytes, bTreeNode);

            assertEquals(expectedFreeSize, bTreeNonLeaf.calculateFreeSpaceLeft(BIG_PAGE_SIZE_BYTES));
        }

        assertEquals(11, bTreeNonLeaf.getPairCount());
        bTreeNonLeaf.removeAtIndex(9); //remove last key item
        assertEquals(10, bTreeNonLeaf.getPairCount());

        usedBytes -= (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
        final int expectedFreeSizeAfterDelete = BIG_PAGE_SIZE_BYTES - BIG_NODE_LOG_SIZE- usedBytes - BTreeNodePage.HEADER_SIZE_BYTES;
        assertEquals(expectedFreeSizeAfterDelete, bTreeNonLeaf.calculateFreeSpaceLeft(BIG_PAGE_SIZE_BYTES));

        for (int i = 0; i < 9; i++)
        {
            final byte[] expectedValue = BinaryHelper.longToBytes(numKeysPerChild * i);
            assertArrayEquals(expectedValue, bTreeNonLeaf.getKey(i));
            assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(i));
        }
        assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(9));

        bTreeNonLeaf.removeAtIndex(2);
        assertEquals(9, bTreeNonLeaf.getPairCount());

        usedBytes -= (Long.BYTES * 2) + BTreeNodePage.CELL_SIZE;
        final int expectedFreeSizeAnotherDelete = BIG_PAGE_SIZE_BYTES - BIG_NODE_LOG_SIZE - usedBytes - BTreeNodePage.HEADER_SIZE_BYTES;
        assertEquals(expectedFreeSizeAnotherDelete, bTreeNonLeaf.calculateFreeSpaceLeft(BIG_PAGE_SIZE_BYTES));

        for (int i = 0; i < 8; i++)
        {
            final byte[] expectedValue = BinaryHelper.longToBytes(numKeysPerChild * ((i >= 2) ? i + 1 : i));
            assertArrayEquals(expectedValue, bTreeNonLeaf.getKey(i));
        }
        assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(8));
    }

    @Test
    void shouldIgnoreRemovingNonExistentChildren()
    {
        try
        {
            bTreeNonLeaf.removeAtIndex(0);
        }
        catch (final AssertionError e)
        {
            assertEquals(0, bTreeNonLeaf.getPairCount());
            assertEquals("removing index 0 when key count is 0", e.getMessage());
        }
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithEvenKeyNumber()
    {
        final int currentPairs = 6;
        final int splitPairs = 5;

        final int totalPairs = 11;

        runSplitTest(currentPairs, splitPairs, 0, 0, totalPairs, 0);
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithOddKeyNumber()
    {
        final int currentPairs = 6;
        final int splitPairs = 6;

        final int totalPairs = 12;
        runSplitTest(currentPairs, splitPairs, 0, 0, totalPairs, 0);
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithEvenKeyNumberAndEvenLogNumber()
    {
        final int currentPairs = 5;
        final int splitPairs = 5;
        final int currentLogPairs = 5;
        final int splitLogPairs = 5;

        final int totalPairs = 10;
        final int totalLogPairs = 10;

        runSplitTest(currentPairs, splitPairs, currentLogPairs, splitLogPairs, totalPairs, totalLogPairs);
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithEvenKeyNumberAndOddLogNumber()
    {
        final int currentPairs = 5;
        final int splitPairs = 5;
        final int currentLogPairs = 5;
        final int splitLogPairs = 6;

        final int totalPairs = 10;
        final int totalLogPairs = 11;

        runSplitTest(currentPairs, splitPairs, currentLogPairs, splitLogPairs, totalPairs, totalLogPairs);
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithOddKeyNumberWithEvenLog()
    {
        final int currentPairs = 6;
        final int splitPairs = 5;
        final int currentLogPairs = 6;
        final int splitLogPairs = 6;

        final int totalPairs = 11;
        final int totalLogPairs = 12;

        runSplitTest(currentPairs, splitPairs, currentLogPairs, splitLogPairs, totalPairs, totalLogPairs);
    }

    @Test
    void shouldBeAbleToSplitInTwoNodesWithOddKeyNumberWithOddLog()
    {
        final int currentPairs = 6;
        final int splitPairs = 5;
        final int currentLogPairs = 6;
        final int splitLogPairs = 5;

        final int totalPairs = 11;
        final int totalLogPairs = 11;

        runSplitTest(currentPairs, splitPairs, currentLogPairs, splitLogPairs, totalPairs, totalLogPairs);
    }

    @Test
    void shouldDeepCopyNonLeafNode()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int key = numKeysPerChild * i;
            final byte[] keyBytes = BinaryHelper.longToBytes(numKeysPerChild * i);
            final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            bTreeNonLeaf.insertChild(i, keyBytes, bTreeNode);

            final int expectedPairs = i + 2;// +1 to convert index to number of and + 1 to account to the rightmost pair
            assertEquals(expectedPairs, bTreeNonLeaf.getPairCount());
        }

        final int maxLogKeyValuePairs = 6;
        generateKeyValuePairs(maxLogKeyValuePairs, (i, pair) -> bTreeNonLeaf.insertLog(pair.key, pair.value));

        final BTreeNodeNonLeaf copy = new BTreeNodeNonLeaf(
                bTreeNonLeaf.getPageNumber(),
                MemoryFactory.allocateHeap(BIG_PAGE_SIZE_BYTES, TestUtils.BYTE_ORDER),
                BIG_NODE_LOG_SIZE,
                0, //there is always one child at least
                null);

        bTreeNonLeaf.copy(copy);

        final int key = numKeysPerChild * 10;
        final byte[] keyBytes = BinaryHelper.longToBytes(key);
        final BTreeNodeLeaf child = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));
        bTreeNonLeaf.insertChild(8, keyBytes, child);

        assertEquals(12, bTreeNonLeaf.getPairCount());
        assertEquals(11, copy.getPairCount());

        generateKeyValuePairs(maxLogKeyValuePairs, (i, pair) ->
        {
            assertArrayEquals(pair.key, copy.getLogKey(i));
            assertArrayEquals(pair.value, copy.getLogValueAtIndex(i));
        });
    }

    @Test
    void shouldBeAbleToSpillKeyValuesFromNodeLog()
    {
        long maxLogKeyValuePairs = 0L;
        while (bTreeNonLeaf.logHasFreeSpace(2 * Long.BYTES))
        {
            final byte[] bytes = BinaryHelper.longToBytes(maxLogKeyValuePairs);
            bTreeNonLeaf.insertLog(bytes, bytes);
            maxLogKeyValuePairs++;
        }

        assertFalse(bTreeNonLeaf.logHasFreeSpace(2 * Long.BYTES));
        assertEquals(maxLogKeyValuePairs, bTreeNonLeaf.getLogKeyValuesCount());

        final KeyValueLogImpl keyValueLog = bTreeNonLeaf.spillLog();

        assertTrue(bTreeNonLeaf.logHasFreeSpace(2 * Long.BYTES));
        assertEquals(0, bTreeNonLeaf.getLogKeyValuesCount());

        for (int i = 0; i < maxLogKeyValuePairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(bytes, keyValueLog.getKeyAtIndex(i));
            assertArrayEquals(bytes, keyValueLog.getValueAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToSpillKeyValuesFromNodeLogWileKeepingOriginalEntries()
    {
        final int numKeysPerChild = 5;
        final int childrenToInsert = 10;
        final int expectedPairCount = childrenToInsert + 1;
        for (int i = 0; i < childrenToInsert; i++)
        {
            final int key = numKeysPerChild * i;
            final KeyValueUtils.Pair pair = generateKeyValuePair(key);
            final BTreeNodeLeaf bTreeNode = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            bTreeNonLeaf.insertChild(i, pair.key, bTreeNode);
        }

        for (int counter = 0; bTreeNonLeaf.logHasFreeSpace(generateKeyValuePair(counter).getTotalLength()); counter++)
        {
            final KeyValueUtils.Pair pair = generateKeyValuePair(counter);
            bTreeNonLeaf.insertLog(pair.key, pair.value);
        }

        bTreeNonLeaf.spillLog();

        assertEquals(expectedPairCount, bTreeNonLeaf.getPairCount());

        for (int i = 0; i < expectedPairCount; i++)
        {
            final int expectedValue = numKeysPerChild * i;

            if (i == childrenToInsert)
            {
                assertArrayEquals(new byte[0], bTreeNonLeaf.getKey(i));
            }
            else
            {
                final KeyValueUtils.Pair pair = generateKeyValuePair(expectedValue);
                assertArrayEquals(pair.key, bTreeNonLeaf.getKey(i));
            }

            assertArrayEquals(NON_COMMITTED_CHILD, bTreeNonLeaf.getValue(i));
        }
    }

    private void runSplitTest(
            final int expectedPairsInCurrent,
            final int expectedPairsInSplit,
            final int expectedLogKeysInCurrent,
            final int expectedLogKeysInSplit,
            final int totalPairs,
            final int totalLogPairs)
    {
        final int numKeysPerChild = 5;
        final int expectedKeysInCurrent = expectedPairsInCurrent - 1;
        final int expectedKeysInSplit = expectedPairsInSplit - 1;
        final int pairsToInsert = totalPairs - 1; // + 1 of what already is in the bTreeNonLeaf node

        for (int i = 0; i < pairsToInsert; i++)
        {
            final int key = numKeysPerChild * i;
            final BTreeNodeLeaf child = TestUtils.createLeafNodeWithKeys(numKeysPerChild, key, new IdSupplier(key));

            final KeyValueUtils.Pair pair = generateKeyValuePair(key);
            final int keyIndex = bTreeNonLeaf.getKeyIndex(pair.key);
            bTreeNonLeaf.insertChild(keyIndex, pair.key, child);
        }

        for (int i = 0; i < totalLogPairs; i++)
        {
            final int key = numKeysPerChild * i;
            final KeyValueUtils.Pair pair = generateKeyValuePair(key);
            bTreeNonLeaf.insertLog(pair.key, pair.value);
        }

        final List<KeyValueUtils.Pair> pairsCurrent = new ArrayList<>();
        final List<KeyValueUtils.Pair> pairsSplit = new ArrayList<>();
        final List<KeyValueUtils.Pair> pairsLogCurrent = new ArrayList<>();
        final List<KeyValueUtils.Pair> pairsLogSplit = new ArrayList<>();
        for (int i = 0; i < expectedPairsInCurrent; i++)
        {
            pairsCurrent.add(new KeyValueUtils.Pair(bTreeNonLeaf.getKey(i), bTreeNonLeaf.getValue(i)));
        }
        for (int i = 0; i < expectedLogKeysInCurrent; i++)
        {
            pairsLogCurrent.add(new KeyValueUtils.Pair(bTreeNonLeaf.getLogKey(i), bTreeNonLeaf.getLogValueAtIndex(i)));
        }

        for (int i = 0; i < expectedPairsInSplit; i++)
        {
            final int index = i + expectedPairsInCurrent;
            pairsSplit.add(new KeyValueUtils.Pair(bTreeNonLeaf.getKey(index), bTreeNonLeaf.getValue(index)));
        }
        for (int i = 0; i < expectedLogKeysInSplit; i++)
        {
            final int index = i + expectedLogKeysInCurrent;
            pairsLogSplit.add(new KeyValueUtils.Pair(bTreeNonLeaf.getLogKey(index), bTreeNonLeaf.getLogValueAtIndex(index)));
        }

        final int at = pairsToInsert >> 1;
        final BTreeNodeNonLeaf split = new BTreeNodeNonLeaf(
                0L,
                MemoryFactory.allocateHeap(BIG_PAGE_SIZE_BYTES, TestUtils.BYTE_ORDER),
                BIG_NODE_LOG_SIZE,
                1, //there is always one child at least
                new BTreeNodeHeap[1]);
        bTreeNonLeaf.split(at, split);

        assertEquals(expectedPairsInCurrent, bTreeNonLeaf.getPairCount());
        assertEquals(expectedLogKeysInCurrent, bTreeNonLeaf.getLogKeyValuesCount());
        assertEquals(expectedPairsInSplit, split.getPairCount());
        assertEquals(expectedLogKeysInSplit, split.getLogKeyValuesCount());

        ////////////////find the key/values in current
        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final KeyValueUtils.Pair pair = pairsCurrent.get(i);
            assertArrayEquals(pair.key, bTreeNonLeaf.getKey(i));
            assertArrayEquals(pair.value, bTreeNonLeaf.getValue(i));
        }
        assertArrayEquals(new byte[0], bTreeNonLeaf.getKey(expectedKeysInCurrent));
        assertArrayEquals(pairsCurrent.get(expectedKeysInCurrent).value, bTreeNonLeaf.getValue(expectedKeysInCurrent));

        //find the log key/values in current
        for (int i = 0; i < expectedLogKeysInCurrent; i++)
        {
            final KeyValueUtils.Pair pair = pairsLogCurrent.get(i);
            assertArrayEquals(pair.key, bTreeNonLeaf.getLogKey(i));
            assertArrayEquals(pair.value, bTreeNonLeaf.getLogValueAtIndex(i));
        }

        ////////////////find the key/values in split
        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final KeyValueUtils.Pair pair = pairsSplit.get(i);
            assertArrayEquals(pair.key, split.getKey(i));
            assertArrayEquals(pair.value, split.getValue(i));
        }
        assertArrayEquals(new byte[0], split.getKey(expectedKeysInSplit));
        assertArrayEquals(pairsSplit.get(expectedKeysInSplit).value, split.getValue(expectedKeysInSplit));

        //find the log key/values in current
        for (int i = 0; i < expectedLogKeysInSplit; i++)
        {
            final KeyValueUtils.Pair pair = pairsLogSplit.get(i);
            assertArrayEquals(pair.key, split.getLogKey(i));
            assertArrayEquals(pair.value, split.getLogValueAtIndex(i));
        }
    }
}