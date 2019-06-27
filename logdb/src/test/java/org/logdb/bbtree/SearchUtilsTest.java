package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchUtilsTest
{
    private MyKeyIndexSupplier keySupplier;
    private long[] keys;

    @BeforeEach
    void setUp()
    {
        keys = new long[]{0, 1, 2, 5};
        keySupplier = new MyKeyIndexSupplier(keys);
    }

    @Test
    void shouldPerformBinarySearch()
    {
        assertEquals(0, SearchUtils.binarySearch(0, keys.length, keySupplier));
        assertEquals(1, SearchUtils.binarySearch(1, keys.length, keySupplier));
        assertEquals(2, SearchUtils.binarySearch(2, keys.length, keySupplier));
        assertEquals(3, SearchUtils.binarySearch(5, keys.length, keySupplier));
    }

    @Test
    void shouldNotFindAKeyBiggerThenAll()
    {
        int expectedIndex = -(keys.length + 1);
        assertEquals(expectedIndex, SearchUtils.binarySearch(6, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(7, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(8, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(9, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(10, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(11, keys.length, keySupplier));
    }

    @Test
    void shouldNotFindAKeySmallerThenAll()
    {
        int expectedIndex = -1;
        assertEquals(expectedIndex, SearchUtils.binarySearch(-1, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-2, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-3, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-4, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-5, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-6, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-7, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-8, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-9, keys.length, keySupplier));
        assertEquals(expectedIndex, SearchUtils.binarySearch(-10, keys.length, keySupplier));
    }

    @Test
    void shouldNotFindKeyInTheMiddle()
    {
        assertEquals(-4, SearchUtils.binarySearch(3, keys.length, keySupplier));
        assertEquals(-4, SearchUtils.binarySearch(4, keys.length, keySupplier));
    }

    static final class MyKeyIndexSupplier implements SearchUtils.KeyIndexSupplier
    {
        final long[] keys;

        MyKeyIndexSupplier(final long[] keys)
        {
            this.keys = keys;
        }

        @Override
        public long getKey(int index)
        {
            return keys[index];
        }
    }
}
