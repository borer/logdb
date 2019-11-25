package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.logfile.LogFile;
import org.logdb.logfile.LogRecordHeader;
import org.logdb.storage.ByteOffset;
import org.logdb.support.TestUtils;

import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.integration.TestIntegrationUtils.createNewLogFile;
import static org.logdb.integration.TestIntegrationUtils.readLogFile;
import static org.logdb.support.Assertions.assertExceptionWithMessage;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class LogFileIntegrationTest
{
    @TempDir Path tempDirectory;
    private static final long HEADER_CHECKSUM_SIZE = Long.BYTES; // crc32 value

    @Test
    void shouldPersistAndReadMultipleSimpleKeyValue() throws Exception
    {
        try (final LogFile logFile = createNewLogFile(tempDirectory))
        {
            final byte[] keyBytes = "key".getBytes();
            final byte[] valueBytes = "value".getBytes();
            final long offset = logFile.put(keyBytes, valueBytes);

            final byte[] keyBytes2 = "key2".getBytes();
            final byte[] valueBytes2 = "value2".getBytes();
            final long offset2 = logFile.put(keyBytes2, valueBytes2);

            final byte[] keyBytes3 = "key3".getBytes();
            final byte[] valueBytes3 = "value3".getBytes();
            final long offset3 = logFile.put(keyBytes3, valueBytes3);

            final byte[] readValue = logFile.read(offset);
            assertArrayEquals(valueBytes, readValue);

            final byte[] readValue2 = logFile.read(offset2);
            assertArrayEquals(valueBytes2, readValue2);

            final byte[] readValue3 = logFile.read(offset3);
            assertArrayEquals(valueBytes3, readValue3);
        }
    }

    @Test
    void shouldPersistAndReadKeyValuesBiggerThanPageSize() throws Exception
    {
        final int length = PAGE_SIZE_BYTES * 2;
        final byte[] keyBytes = generateByteArray(length);
        final byte[] valueBytes = generateByteArray(length);

        try (final LogFile logFile = createNewLogFile(tempDirectory))
        {
            final long offset = logFile.put(keyBytes, valueBytes);

            final byte[] readValue = logFile.read(offset);

            assertArrayEquals(valueBytes, readValue);
        }
    }

    @Test
    void shouldBeAbleToDeleteKeyValue() throws Exception
    {
        final byte[] keyBytes = "key".getBytes();
        final byte[] valueBytes = "value".getBytes();

        try (final LogFile logFile = createNewLogFile(tempDirectory))
        {
            final long offset = logFile.put(keyBytes, valueBytes);

            final @ByteOffset long removeOffset = logFile.delete(keyBytes);

            final byte[] readValue = logFile.read(offset);
            assertArrayEquals(valueBytes, readValue);

            assertExceptionWithMessage(
                    "offset 1578 refers to a delete record",
                    () -> logFile.read(removeOffset));
        }
    }

    @Test
    void shouldPersistAndLoadLogFile() throws Exception
    {
        final int numberOfPairs = 100;
        final long[] offsets = new long[numberOfPairs];

        try (final LogFile logFile = createNewLogFile(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs; i++)
            {
                final String key = "key" + i;
                final String value = "value" + i;
                final byte[] keyBytes = key.getBytes();
                final byte[] valueBytes = value.getBytes();
                offsets[i] = logFile.put(keyBytes, valueBytes);
            }
        }

        try (final LogFile logFile = readLogFile(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs; i++)
            {
                final String expectedValue = "value" + i;
                final byte[] readValue = logFile.read(offsets[i]);
                assertArrayEquals(expectedValue.getBytes(), readValue);
            }
        }
    }

    @Test
    void shouldPersistAndLoadLogFileWithBigEndianByteOrder() throws Exception
    {
        final int numberOfPairs = 100;
        final long[] offsets = new long[numberOfPairs];

        try (LogFile logFile = createNewLogFile(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            for (int i = 0; i < numberOfPairs; i++)
            {
                final String key = "key" + i;
                final String value = "value" + i;
                final byte[] keyBytes = key.getBytes();
                final byte[] valueBytes = value.getBytes();
                offsets[i] = logFile.put(keyBytes, valueBytes);
            }
        }

        try (final LogFile logFile = readLogFile(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs; i++)
            {
                final String expectedValue = "value" + i;
                final byte[] readValue = logFile.read(offsets[i]);
                assertArrayEquals(expectedValue.getBytes(), readValue);
            }
        }
    }

    @Test
    void shouldAlwaysPersistRecordsSequentially() throws Exception
    {
        final int numberOfPairs = 100;

        long previousOffset = 0;
        long previousKeyLength = 0;
        long previousValueLength = 0;
        long lastSegmentSize = 0;

        try (final LogFile logFile = createNewLogFile(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs; i++)
            {
                final String key = "key" + i;
                final String value = "value" + i;
                final byte[] keyBytes = key.getBytes();
                final byte[] valueBytes = value.getBytes();
                final long actualOffset = logFile.put(keyBytes, valueBytes);

                //assert that the log records are sequentially stored, skip first because of the file header
                if (i != 0)
                {
                    final long expectedOffset = previousOffset +
                            LogRecordHeader.RECORD_HEADER_STATIC_SIZE + HEADER_CHECKSUM_SIZE +
                            previousKeyLength +
                            previousValueLength;

                    assertEquals(expectedOffset, actualOffset);
                }

                previousOffset = actualOffset;
                previousKeyLength = keyBytes.length;
                previousValueLength = valueBytes.length;

                if (TestUtils.SEGMENT_FILE_SIZE < (actualOffset - lastSegmentSize))
                {
                    lastSegmentSize = actualOffset;
                    previousOffset += PAGE_SIZE_BYTES;//header new file
                }
            }
        }

        try (final LogFile logFile = readLogFile(tempDirectory))
        {
            //should be able to continue persist
            for (int i = numberOfPairs; i < numberOfPairs * 2; i++)
            {
                final String key = "key" + i;
                final String value = "value" + i;
                final byte[] keyBytes = key.getBytes();
                final byte[] valueBytes = value.getBytes();
                final long actualOffset = logFile.put(keyBytes, valueBytes);

                //assert that the log records are sequentially stored
                final long expectedOffset = previousOffset +
                        LogRecordHeader.RECORD_HEADER_STATIC_SIZE + HEADER_CHECKSUM_SIZE +
                        previousKeyLength +
                        previousValueLength;

                assertEquals(expectedOffset, actualOffset);

                previousOffset = actualOffset;
                previousKeyLength = keyBytes.length;
                previousValueLength = valueBytes.length;
            }
        }
    }

    private byte[] generateByteArray(final int length)
    {
        final byte[] buffer = new byte[length];
        for (int i = 0; i < length; i++)
        {
            buffer[i] = (byte) i;
        }
        return buffer;
    }
}