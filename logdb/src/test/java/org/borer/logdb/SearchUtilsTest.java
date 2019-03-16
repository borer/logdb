package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchUtilsTest
{
    private ByteBuffer key0;
    private ByteBuffer key1;
    private ByteBuffer key2;
    private ByteBuffer key3;
    private ByteBuffer[] array;

    @BeforeEach
    void setUp()
    {
        key0 = createValue("0");
        key1 = createValue("1");
        key2 = createValue("2");
        key3 = createValue("3");
        array = new ByteBuffer[] {key0, key1, key2, key3};
    }

    @Test
    void shouldPerformBinarySearch()
    {
        assertEquals(0, SearchUtils.binarySearch(key0, array));
        assertEquals(1, SearchUtils.binarySearch(key1, array));
        assertEquals(2, SearchUtils.binarySearch(key2, array));
        assertEquals(3, SearchUtils.binarySearch(key3, array));
    }

    @Test
    void shouldNotFindAKeyBiggerThenAll()
    {
        int expectedIndex = -(array.length + 1);
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("4"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("5"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("6"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("7"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("8"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("9"), array));
    }

    @Test
    void shouldNotFindAKeySmallerThenAll()
    {
        int expectedIndex = -1;
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-1"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-2"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-3"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-4"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-5"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-6"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-7"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-8"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-9"), array));
        assertEquals(expectedIndex, SearchUtils.binarySearch(createValue("-10"), array));
    }

    @Test
    void shouldNotFindKeyInTheMiddle()
    {
        assertEquals(-3, SearchUtils.binarySearch(createValue("11"), array));
        assertEquals(-4, SearchUtils.binarySearch(createValue("22"), array));
    }
}
