package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BTreeNodeLeafTest
{
    private BTreeNodeLeaf bTreeLeaf;

    @BeforeEach
    void setUp()
    {
        bTreeLeaf = new BTreeNodeLeaf();
    }

    /////////////////////////////////Add/Update

    @Test
    void shouldBeAbleToInsertNewValue()
    {
        final ByteBuffer key = createValue("key");
        final ByteBuffer value = createValue("value");
        bTreeLeaf.insert(key, value);

        assertEquals(key, bTreeLeaf.getKeyAtIndex(0));
        assertEquals(value, bTreeLeaf.getValueAtIndex(0));
        assertEquals(1, bTreeLeaf.getKeyCount());
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValues()
    {
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }
    }

    @Test
    void shouldBeAbleToInsertMultipleNewValuesInMemOrder()
    {
        final int count = 10;

        for (int i = count - 1; i >= 0; i--)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);
        }

        assertEquals(count, bTreeLeaf.getKeyCount());

        for (int i = 0; i < count; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
        }
    }

    @Test
    void shouldBeAbleToUpdateValuesWithExistingKey()
    {
        final ByteBuffer key = createValue("key");
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(0));
            assertEquals(value, bTreeLeaf.getValueAtIndex(0));
            assertEquals(1, bTreeLeaf.getKeyCount());
        }
    }

    /////////////////////////////////Remove

    @Test
    void shouldBeAbleToRemoveEntry()
    {
        final ByteBuffer key = createValue("key");
        final ByteBuffer value = createValue("value");

        bTreeLeaf.insert(key, value);

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
    void shouldBeAbleToRemoveMultipleEntries()
    {
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            bTreeLeaf.remove(0);
            assertEquals(i, bTreeLeaf.getKeyCount());
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
        final ByteBuffer key = createValue("key");
        final ByteBuffer value = createValue("value");

        bTreeLeaf.insert(key, value);

        final ByteBuffer valueFound = bTreeLeaf.get(key);

        assertEquals(value, valueFound);
    }

    @Test
    void shouldGetNullForKeyNotFound()
    {
        final ByteBuffer key = createValue("key");

        final ByteBuffer valueFound = bTreeLeaf.get(key);

        assertNull(valueFound);
    }

    /////////////////////////////////Split

    @Test
    void shouldBeAbleToSplitALeaf()
    {
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);
        }

        assertEquals(10, bTreeLeaf.getKeyCount());

        final BTreeNodeLeaf newBtree = (BTreeNodeLeaf)bTreeLeaf.split(5);

        assertEquals(5, bTreeLeaf.getKeyCount());
        for (int i = 0; i < 5; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);
            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
        }

        assertEquals(5, newBtree.getKeyCount());
        for (int i = 0; i < 5; i++)
        {
            final ByteBuffer key = createValue("key" + (i + 5));
            final ByteBuffer value = createValue("value" + (i + 5));
            assertEquals(key, newBtree.getKeyAtIndex(i));
            assertEquals(value, newBtree.getValueAtIndex(i));
        }
    }

    @Test
    void shouldDeepCopyLeafNode()
    {
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);
        }

        final BTreeNodeLeaf copy = (BTreeNodeLeaf)bTreeLeaf.copy();

        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("valueEdited" + i);

            bTreeLeaf.insert(key, value);
        }

        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            assertEquals(key, copy.getKeyAtIndex(i));
            assertEquals(value, copy.getValueAtIndex(i));
        }
    }
}