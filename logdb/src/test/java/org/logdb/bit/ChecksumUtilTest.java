package org.logdb.bit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.checksum.ChecksumUtil;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChecksumUtilTest
{
    private ChecksumUtil checksumUtil;
    private byte[] buffer;

    @BeforeEach
    void setUp()
    {
        buffer = "this is a test".getBytes();
        checksumUtil = new ChecksumUtil();
    }

    @Test
    void shouldCompareCorrectlySameData()
    {
        final int originalChecksum = checksumUtil.calculateSingleChecksum(
                buffer, 0, buffer.length);

        assertTrue(checksumUtil.compareSingleChecksum(
                originalChecksum,
                buffer,
                0,
                buffer.length));
    }

    @Test
    void shouldCompareCorrectlyDifferentData()
    {
        final int originalChecksum = checksumUtil.calculateSingleChecksum(
                buffer, 0, buffer.length);
        final int skipElements = 1;

        assertFalse(checksumUtil.compareSingleChecksum(
                originalChecksum,
                buffer,
                skipElements,
                buffer.length - skipElements));
    }

    @Test
    void shouldCompareCorrectlyOverMultipleAppends()
    {
        checksumUtil.updateChecksum(buffer, 0, buffer.length);
        checksumUtil.updateChecksum(1234L);
        final int checksum1 = checksumUtil.getAndResetChecksum();

        checksumUtil.updateChecksum(buffer, 0, buffer.length);
        checksumUtil.updateChecksum(1234L);
        final int checksum2 = checksumUtil.getAndResetChecksum();

        assertTrue(checksumUtil.compareChecksum(checksum1, checksum2));
    }
}