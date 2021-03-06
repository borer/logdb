package org.logdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.LogDb;
import org.logdb.bit.BinaryHelper;
import org.logdb.builder.LogDbBuilder;
import org.logdb.storage.Version;
import org.logdb.storage.file.FileStorageUtil;
import org.logdb.storage.file.FileType;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

abstract class LogDbBaseIntegrationTest
{
    @TempDir Path tempDirectory;
    private LogDb logDB;

    private final boolean shouldUseIndexWithLog;

    protected LogDbBaseIntegrationTest(boolean shouldUseIndexWithLog)
    {
        this.shouldUseIndexWithLog = shouldUseIndexWithLog;
    }

    @BeforeEach
    void setUp() throws IOException
    {
        final LogDbBuilder logDbBuilder = new LogDbBuilder();
        logDB = logDbBuilder
                .setRootDirectory(tempDirectory)
                .setTimeSource(new StubTimeSource())
                .setByteOrder(TestUtils.BYTE_ORDER)
                .setSegmentFileSize(TestUtils.SEGMENT_FILE_SIZE)
                .setPageSizeBytes(TestUtils.PAGE_SIZE_BYTES)
                .pageLogSize(TestUtils.NODE_LOG_SIZE)
                .useIndexWithLog(shouldUseIndexWithLog)
                .pageLogSize(TestUtils.NODE_LOG_SIZE)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        logDB.close();
    }

    @Test
    void shouldPersistsAndGetFromDB() throws IOException
    {
        final int numOfPairs = 10;
        for (int i = 0; i < numOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
            logDB.commitIndex();
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            final String expectedValue = buildExpectedValue(i);
            final byte[] key = BinaryHelper.longToBytes(i);
            final byte[] value = logDB.get(key);
            assertArrayEquals(expectedValue.getBytes(), value);
        }
    }

    @Test
    void shouldPersistsAndGetHistoricValuesFromDB() throws IOException
    {
        final byte[] key = BinaryHelper.longToBytes(123123123L);
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
    void shouldPersistsAndDeleteFromDB() throws IOException
    {
        final int numOfPairs = 10;
        for (int i = 0; i < numOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
            logDB.commitIndex();
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            logDB.delete(key);
        }

        for (int i = 0; i < numOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            assertNull(logDB.get(key));
        }
    }

    @Test
    void shouldHaveLogAndIndexVersionsInSync() throws IOException
    {
        final int numOfPutPairs = 10;
        for (int i = 0; i < numOfPutPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            final String value = buildExpectedValue(i);
            final byte[] valueBytes = value.getBytes();
            logDB.put(key, valueBytes);
            logDB.commitIndex();
        }

        final int numOfDeletePairs = 5;
        for (int i = 0; i < numOfDeletePairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            logDB.delete(key);
            logDB.commitIndex();
        }

        final int expectedVersion = 14;

        final @Version long indexVersion = FileStorageUtil.getLastAppendVersion(tempDirectory, FileType.INDEX);
        final @Version long heapVersion = FileStorageUtil.getLastAppendVersion(tempDirectory, FileType.HEAP);

        assertEquals(indexVersion, heapVersion);
        assertEquals(expectedVersion, indexVersion);
        assertEquals(expectedVersion, heapVersion);
    }

    private String buildExpectedValue(int i)
    {
        return "expectedValue" + i;
    }
}
