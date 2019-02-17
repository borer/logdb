package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createLeafNodeWithKeys;
import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BTreeNodeNonLeafTest
{
    private BTreeNodeNonLeaf bTreeNonLeaf;

    @BeforeEach
    void setUp()
    {
        bTreeNonLeaf = new BTreeNodeNonLeaf();
    }

    @Test
    void shouldBeAbleToInsertChild()
    {
        final BTreeNode bTreeNode = createLeafNodeWithKeys(10, 0);
        final ByteBuffer key = createValue("key0");

        bTreeNonLeaf.insertChild(0, key, bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }
    }

    @Test
    void shouldBeAbleToDeleteChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);
        }

        assertEquals(10, bTreeNonLeaf.getKeyCount());

        bTreeNonLeaf.remove(9);
        assertEquals(9, bTreeNonLeaf.getKeyCount());

        bTreeNonLeaf.remove(2);
        assertEquals(8, bTreeNonLeaf.getKeyCount());
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
    void shouldBeAbleToSplitInTwoNodes()
    {
        final int numKeysPerChild = 5;
        final int expectedKeysInCurrent = 5;
        final int expectedKeysInSplit = 4;
        for (int i = 0; i < 10; i++)
        {
            final String keyValue = "key" + (numKeysPerChild * i);
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final BTreeNode child = new BTreeNodeLeaf();

            bTreeNonLeaf.insertChild(i, childKeyBuffer, child);
        }

        //first 5 children have first 25 keys
        final ByteBuffer keyUsedForSplit = bTreeNonLeaf.getKeyAtIndex(5);

        final BTreeNodeNonLeaf split = (BTreeNodeNonLeaf) bTreeNonLeaf.split(5);

        assertEquals(expectedKeysInCurrent, bTreeNonLeaf.getKeyCount());
        assertEquals(expectedKeysInSplit, split.getKeyCount());

        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final ByteBuffer keyAtIndex = bTreeNonLeaf.getKeyAtIndex(i);
            assertNotEquals(keyUsedForSplit, keyAtIndex);
        }

        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final ByteBuffer keyAtIndex = split.getKeyAtIndex(i);
            assertNotEquals(keyUsedForSplit, keyAtIndex);
        }
    }

    @Test
    void shouldDeepCopyNonLeafNode()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }

        BTreeNode copy = bTreeNonLeaf.copy();

        final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * 10));
        final ByteBuffer key = createValue("key" + (numKeysPerChild * 10));
        bTreeNonLeaf.insertChild(8, key, child);

        assertEquals(11, bTreeNonLeaf.getKeyCount());
        assertEquals(10, copy.getKeyCount());
    }
}