package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeWithLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;
import static org.logdb.integration.TestIntegrationUtils.createNewPersistedLogBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedLogBtree;

public class BBTreeWithLogIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BBTreeWithLogIntegrationTest.class);

    @TempDir
    Path tempDirectory;

    @Test
    void shouldNotFindKeyLowerThatAnyOtherKey() throws Exception
    {
        final long nonExistingKeyValuePair = -1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.commit();
            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldNotFindKeyBiggerThatAnyOtherKey() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.commit();
            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldBeABleToCreateNewDBWithLogFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.put(10, 10);
            bTree.put(5, 5);

            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(tempDirectory))
        {
            assertEquals(1, readBTree.get(1));
            assertEquals(10, readBTree.get(10));
            assertEquals(5, readBTree.get(5));

            assertEquals(KEY_NOT_FOUND_VALUE, readBTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDBWithLog() throws IOException
    {
        final long key = 123L;
        final int maxVersions = 100;
        final long nonExistingKey = 964L;

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
                originalBTree.commit();
            }

            assertEquals(KEY_NOT_FOUND_VALUE, originalBTree.get(nonExistingKey));
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < maxVersions; i++)
            {
                assertEquals(i, loadedBTree.get(key, i));
            }

            assertEquals(KEY_NOT_FOUND_VALUE, loadedBTree.get(nonExistingKey));
        }
    }

    @Test
    void shouldBeABleToCommitMultipleTimesWithLog() throws IOException
    {
        final int numberOfPairs = 600;

        final String original;
        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {

            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }

            original = originalBTree.print();
            originalBTree.commit();
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            final String loaded = loadedBTree.print();

            assertEquals(original, loaded);

            for (int i = 0; i < numberOfPairs; i++)
            {
                assertEquals(i, loadedBTree.get(i));
            }
        }
    }

    @Test
    void shouldBeABleToLoadAndContinuePersistingBtreeWithLog() throws IOException
    {
        final int numberOfPairs = 300;

        //create new tree and insert elements
        try (BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }
            originalBTree.commit();
        }

        //open the persisted tree and continue inserting elements
        try (BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = numberOfPairs; i < numberOfPairs * 2; i++)
            {
                loadedBTree.put(i, i);
                loadedBTree.commit();
            }
        }

        //open the persisted tree and read all the elements
        try (BTreeWithLog loadedBTree2 = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs * 2; i++)
            {
                assertEquals(i, loadedBTree2.get(i));
            }
        }
    }
}
