package org.logdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.LogDb;
import org.logdb.LogDbBuilder;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LogDbIntegrationTest
{
    @TempDir Path tempDirectory;
    private LogDb logDB;

    @BeforeEach
    void setUp() throws IOException
    {
        final LogDbBuilder logDbBuilder = new LogDbBuilder();
        logDB = logDbBuilder
                .setDbName("test")
                .setRootDirectory(tempDirectory.toFile().getAbsolutePath())
                .setTimeSource(new StubTimeSource())
                .setByteOrder(TestUtils.BYTE_ORDER)
                .setMemoryMappedChunkSizeBytes(TestUtils.MAPPED_CHUNK_SIZE)
                .setPageSizeBytes(TestUtils.PAGE_SIZE_BYTES)
                .build();
    }

    @AfterEach
    void tearDown()
    {
        logDB.close();
    }

    @Test
    void shouldPersistsAndGetFromDB()
    {
        final int numOfPairs = 10;
        for (int i = 0; i < numOfPairs; i++)
        {
            final long key = (long)i;
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
            logDB.commitIndex();
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            final String expectedValue = buildExpectedValue(i);
            final byte[] readBytes = logDB.get(i);
            assertArrayEquals(expectedValue.getBytes(), readBytes);
        }
    }

    @Test
    void shouldPersistsAndGetHistoricValuesFromDB()
    {
        final long key = 123123123L;
        final int numOfPairs = 10;
        for (int i = 0; i < numOfPairs; i++)
        {
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
        }

        logDB.commitIndex();

        for (int i = 0; i < numOfPairs; i++)
        {
            final byte[] readBytes = logDB.get(key, i);
            final String expectedValue = buildExpectedValue(i);
            assertArrayEquals(expectedValue.getBytes(), readBytes);
        }
    }

    @Test
    void shouldPersistsAndDeleteFromDB()
    {
        final int numOfPairs = 10;
        for (int i = 0; i < numOfPairs; i++)
        {
            final long key = (long)i;
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
            logDB.commitIndex();
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            logDB.delete(i);
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            assertNull(logDB.get(i));
        }
    }

    private String buildExpectedValue(int i)
    {
        return "expectedValue" + i;
    }
}
