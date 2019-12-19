package org.logdb.bit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteArrayComparatorTest
{
    @Test
    void shouldCompareByteArrays()
    {
        final byte[] bytesA = BinaryHelper.longToBytes(150);
        final byte[] bytesB = BinaryHelper.longToBytes(250);

        assertAIsSmaller(bytesA, bytesB);
        assertAIsBigger(bytesB, bytesA);
        assertBothEqual(bytesB, bytesB);
        assertBothEqual(bytesA, bytesA);
    }

    @Test
    void shouldCompareByteArraysWithDifferentLength()
    {
        final byte[] bytesA = "test".getBytes();
        final byte[] bytesB = BinaryHelper.longToBytes(250);

        assertAIsSmaller(bytesA, bytesB);
        assertAIsBigger(bytesB, bytesA);
        assertBothEqual(bytesB, bytesB);
        assertBothEqual(bytesA, bytesA);
    }

    private void assertAIsBigger(final byte[] a, final byte[] b)
    {
        assertTrue(ByteArrayComparator.INSTANCE.compare(a, b) > 0);
    }

    private void assertAIsSmaller(final byte[] a, final byte[] b)
    {
        assertTrue(ByteArrayComparator.INSTANCE.compare(a, b) < 0);
    }

    private void assertBothEqual(final byte[] a, final byte[] b)
    {
        assertEquals(0, ByteArrayComparator.INSTANCE.compare(a, b));
    }
}