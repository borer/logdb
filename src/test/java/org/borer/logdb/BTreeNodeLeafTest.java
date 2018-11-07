package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

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
        final String keyLabel = "key";
        final String valueLabel = "value";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
        final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());
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
            final String keyLabel = "key" + i;
            final String valueLabel = "value" + i;
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

            bTreeLeaf.insert(key, value);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }
    }

    @Test
    void shouldBeAbleToUpdateValuesWithExistingKey()
    {
        final String keyLabel = "key";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
        for (int i = 0; i < 10; i++)
        {
            final String valueLabel = "value" + i;
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

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
        final String keyLabel = "key";
        final String valueLabel = "value";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
        final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

        bTreeLeaf.insert(key, value);

        assertEquals(1, bTreeLeaf.getKeyCount());

        bTreeLeaf.remove(key);

        assertEquals(0, bTreeLeaf.getKeyCount());
    }

    @Test
    void shouldBeAbleToRemoveMultipleEntries()
    {
        for (int i = 0; i < 10; i++)
        {
            final String keyLabel = "key" + i;
            final String valueLabel = "value" + i;
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

            bTreeLeaf.insert(key, value);

            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
            assertEquals(i + 1, bTreeLeaf.getKeyCount());
        }

        for (int i = 9; i >= 0; i--)
        {
            final String keyLabel = "key" + i;
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());

            bTreeLeaf.remove(key);

            assertEquals(i, bTreeLeaf.getKeyCount());
        }
    }

    @Test
    void shouldIgnoreRemovingNonExistentEntry()
    {
        final String keyLabel = "key";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());

        bTreeLeaf.remove(key);

        assertEquals(0, bTreeLeaf.getKeyCount());
    }

    /////////////////////////////////Get

    @Test
    void shouldBeAbleToGetValueByKey()
    {
        final String keyLabel = "key";
        final String valueLabel = "value";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
        final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

        bTreeLeaf.insert(key, value);

        final ByteBuffer valueFound = bTreeLeaf.get(key);

        assertEquals(value, valueFound);
    }

    @Test
    void shouldGetNullForKeyNotFound()
    {
        final String keyLabel = "key";
        final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());

        final ByteBuffer valueFound = bTreeLeaf.get(key);

        assertNull(valueFound);
    }

    /////////////////////////////////Split

    @Test
    void shouldBeAbleToSplitALeaf()
    {
        for (int i = 0; i < 10; i++)
        {
            final String keyLabel = "key" + i;
            final String valueLabel = "value" + i;
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());

            bTreeLeaf.insert(key, value);
        }

        assertEquals(10, bTreeLeaf.getKeyCount());

        final BTreeNodeLeaf newBtree = (BTreeNodeLeaf)bTreeLeaf.split(5);

        assertEquals(5, bTreeLeaf.getKeyCount());
        for (int i = 0; i < 5; i++)
        {
            final String keyLabel = "key" + i;
            final String valueLabel = "value" + i;
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());
            key.rewind();
            value.rewind();
            assertEquals(key, bTreeLeaf.getKeyAtIndex(i));
            assertEquals(value, bTreeLeaf.getValueAtIndex(i));
        }

        assertEquals(5, newBtree.getKeyCount());
        for (int i = 0; i < 5; i++)
        {
            final String keyLabel = "key" + (i + 5);
            final String valueLabel = "value" + (i + 5);
            final ByteBuffer key = ByteBuffer.allocate(keyLabel.length()).put(keyLabel.getBytes());
            final ByteBuffer value = ByteBuffer.allocate(valueLabel.length()).put(valueLabel.getBytes());
            key.rewind();
            value.rewind();
            assertEquals(key, newBtree.getKeyAtIndex(i));
            assertEquals(value, newBtree.getValueAtIndex(i));
        }
    }
}