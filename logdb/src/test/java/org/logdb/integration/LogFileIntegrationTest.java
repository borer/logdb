package org.logdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.logfile.LogFile;
import org.logdb.storage.ByteOffset;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.logdb.integration.TestIntegrationUtils.createNewLogFile;
import static org.logdb.support.Assertions.assertExceptionWithMessage;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class LogFileIntegrationTest
{
    private LogFile logFile;
    @TempDir Path tempDirectory;

    @BeforeEach
    void setUp()
    {
        final File file = tempDirectory.resolve("test.logdb").toFile();
        logFile = createNewLogFile(file);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            logFile.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void shouldPersistAndReadMultipleSimpleKeyValue()
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

    @Test
    void shouldPersistAndReadKeyValuesBiggerThanPageSize()
    {
        final int length = PAGE_SIZE_BYTES * 2;
        final byte[] keyBytes = generateByteArray(length);
        final byte[] valueBytes = generateByteArray(length);

        final long offset = logFile.put(keyBytes, valueBytes);

        final byte[] readValue = logFile.read(offset);

        assertArrayEquals(valueBytes, readValue);
    }

    @Test
    void shouldBeAbleToDeleteKeyValue()
    {
        final byte[] keyBytes = "key".getBytes();
        final byte[] valueBytes = "value".getBytes();
        final long offset = logFile.put(keyBytes, valueBytes);

        final @ByteOffset long removeOffset = logFile.remove(keyBytes);

        final byte[] readValue = logFile.read(offset);
        assertArrayEquals(valueBytes, readValue);

        assertExceptionWithMessage(
                "offset 550 refers to a delete record",
                () -> logFile.read(removeOffset));
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