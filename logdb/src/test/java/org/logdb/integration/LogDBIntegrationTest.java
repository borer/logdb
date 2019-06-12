package org.logdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.LogDB;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.logfile.LogFile;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.logdb.integration.TestIntegrationUtils.createNewLogFile;
import static org.logdb.integration.TestIntegrationUtils.createNewPersistedLogBtree;

public class LogDBIntegrationTest
{
    @TempDir Path tempDirectory;
    private LogDB logDB;

    @BeforeEach
    void setUp()
    {
        final File fileDB = tempDirectory.resolve("test.logdb").toFile();
        final LogFile logFile = createNewLogFile(fileDB);

        final File fileIndex = tempDirectory.resolve("test.logindex").toFile();
        BTreeWithLog index = createNewPersistedLogBtree(fileIndex);

        logDB = new LogDB(logFile, index);
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
