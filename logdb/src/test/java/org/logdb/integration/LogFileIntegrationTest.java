package org.logdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.logfile.LogFile;
import org.logdb.storage.FileStorage;
import org.logdb.support.TestUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.logdb.support.Assertions.assertByteArrayEquals;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class LogFileIntegrationTest
{
    private LogFile logFile;
    private FileStorage storage;
    @TempDir Path tempDirectory;

    @BeforeEach
    void setUp()
    {
        storage = FileStorage.createNewFileDb(
                tempDirectory.resolve("test.logdb").toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        logFile = new LogFile(storage);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            storage.close();
        }
        catch (IOException e)
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
        assertByteArrayEquals(valueBytes, readValue);

        final byte[] readValue2 = logFile.read(offset2);
        assertByteArrayEquals(valueBytes2, readValue2);

        final byte[] readValue3 = logFile.read(offset3);
        assertByteArrayEquals(valueBytes3, readValue3);
    }

    @Test
    void shouldPersistAndReadKeyValuesBiggerThanPageSize()
    {
        final int length = (int) storage.getPageSize() * 2;
        final byte[] keyBytes = generateByteArray(length);
        final byte[] valueBytes = generateByteArray(length);

        final long offset = logFile.put(keyBytes, valueBytes);

        final byte[] readValue = logFile.read(offset);

        assertByteArrayEquals(valueBytes, readValue);
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