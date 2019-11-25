package org.logdb.bit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChecksumHelperTest
{
    private ChecksumHelper checksumHelper;
    private byte[] buffer;

    @BeforeEach
    void setUp()
    {
        buffer = "this is a test".getBytes();
        checksumHelper = new ChecksumHelper(new Crc32(), ChecksumType.CRC32);
    }

    @Test
    void shouldCompareCorrectlySameData()
    {
        final byte[] originalChecksum = checksumHelper.calculateSingleChecksum(buffer, 0, buffer.length);

        assertTrue(checksumHelper.compareSingleChecksum(
                originalChecksum,
                buffer,
                0,
                buffer.length));
    }

    @Test
    void shouldCompareCorrectlyDifferentData()
    {
        final byte[] originalChecksum = checksumHelper.calculateSingleChecksum(buffer, 0, buffer.length);
        final int skipElements = 1;

        assertFalse(checksumHelper.compareSingleChecksum(
                originalChecksum,
                buffer,
                skipElements,
                buffer.length - skipElements));
    }

    @Test
    void shouldCompareCorrectlyOverMultipleAppends()
    {
        checksumHelper.updateChecksum(buffer, 0, buffer.length);
        checksumHelper.updateChecksum(1234L);
        final byte[] checksum1 = checksumHelper.getAndResetChecksum();

        checksumHelper.updateChecksum(buffer, 0, buffer.length);
        checksumHelper.updateChecksum(1234L);
        final byte[] checksum2 = checksumHelper.getAndResetChecksum();

        assertArrayEquals(checksum1, checksum2);
    }
}