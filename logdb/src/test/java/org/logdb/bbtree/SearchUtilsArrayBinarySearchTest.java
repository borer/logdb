package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SearchUtilsArrayBinarySearchTest
{
    private static final byte[] NEGATIVE_TEN = BinaryHelper.longToBytes(-10);
    private static final byte[] NEGATIVE_NINE = BinaryHelper.longToBytes(-9);
    private static final byte[] NEGATIVE_EIGHT = BinaryHelper.longToBytes(-8);
    private static final byte[] NEGATIVE_SEVEN = BinaryHelper.longToBytes(-7);
    private static final byte[] NEGATIVE_SIX = BinaryHelper.longToBytes(-6);
    private static final byte[] NEGATIVE_FIVE = BinaryHelper.longToBytes(-5);
    private static final byte[] NEGATIVE_FOUR = BinaryHelper.longToBytes(-4);
    private static final byte[] NEGATIVE_THREE = BinaryHelper.longToBytes(-3);
    private static final byte[] NEGATIVE_TWO = BinaryHelper.longToBytes(-2);
    private static final byte[] NEGATIVE_ONE = BinaryHelper.longToBytes(-1);
    private static final byte[] ZERO = BinaryHelper.longToBytes(0);
    private static final byte[] ONE = BinaryHelper.longToBytes(1);
    private static final byte[] TWO = BinaryHelper.longToBytes(2);
    private static final byte[] THREE = BinaryHelper.longToBytes(3);
    private static final byte[] FOUR = BinaryHelper.longToBytes(4);
    private static final byte[] FIVE = BinaryHelper.longToBytes(5);
    private static final byte[] SIX = BinaryHelper.longToBytes(6);
    private static final byte[] SEVEN = BinaryHelper.longToBytes(7);
    private static final byte[] EIGHT = BinaryHelper.longToBytes(8);
    private static final byte[] NINE = BinaryHelper.longToBytes(9);
    private static final byte[] TEN = BinaryHelper.longToBytes(10);
    private static final byte[] ELEVEN = BinaryHelper.longToBytes(11);
    private MyKeyIndexSupplier keySupplier;
    private Comparator<byte[]> comparator;
    private byte[][] keys;

    @BeforeEach
    void setUp()
    {
        keys = new byte[][]{ZERO, ONE, TWO, FIVE};
        comparator = ByteArrayComparator.INSTANCE;
        keySupplier = new MyKeyIndexSupplier(keys);
    }

    @Test
    void shouldPerformBinarySearchInOneElementArray()
    {
        assertEquals(0, SearchUtils.binarySearch(ZERO, 1, index -> ZERO, comparator));
        assertEquals(-2, SearchUtils.binarySearch(ONE, 1, index -> ZERO, comparator));
        assertEquals(-2, SearchUtils.binarySearch(NEGATIVE_ONE, 1, index -> ZERO, comparator));
        assertEquals(-2, SearchUtils.binarySearch(NEGATIVE_TWO, 1, index -> ZERO, comparator));
    }

    @Test
    void shouldPerformBinarySearch()
    {
        assertEquals(0, SearchUtils.binarySearch(ZERO, keys.length, keySupplier, comparator));
        assertEquals(1, SearchUtils.binarySearch(ONE, keys.length, keySupplier, comparator));
        assertEquals(2, SearchUtils.binarySearch(TWO, keys.length, keySupplier, comparator));
        assertEquals(3, SearchUtils.binarySearch(FIVE, keys.length, keySupplier, comparator));
    }

    @Test
    void shouldNotFindAKeyBiggerThenAll()
    {
        int expectedIndex = -(keys.length + 1);
        assertEquals(expectedIndex, SearchUtils.binarySearch(SIX, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(SEVEN, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(EIGHT, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NINE, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(TEN, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(ELEVEN, keys.length, keySupplier, comparator));
    }

    @Test
    void shouldNotFindAKeySmallerThenAll()
    {
        int expectedIndex = -5;
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_ONE, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_TWO, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_THREE, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_FOUR, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_FIVE, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_SIX, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_SEVEN, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_EIGHT, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_NINE, keys.length, keySupplier, comparator));
        assertEquals(expectedIndex, SearchUtils.binarySearch(NEGATIVE_TEN, keys.length, keySupplier, comparator));
    }

    @Test
    void shouldNotFindKeyInTheMiddle()
    {
        assertEquals(-4, SearchUtils.binarySearch(THREE, keys.length, keySupplier, comparator));
        assertEquals(-4, SearchUtils.binarySearch(FOUR, keys.length, keySupplier, comparator));
    }

    static final class MyKeyIndexSupplier implements SearchUtils.KeyIndexSupplier<byte[]>
    {
        final byte[][] keys;

        MyKeyIndexSupplier(final byte[][] keys)
        {
            this.keys = keys;
        }

        @Override
        public byte[] getKey(int index)
        {
            return keys[index];
        }
    }
}
