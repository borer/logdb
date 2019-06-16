package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeWithLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    void shouldBeABleToCreateNewDBWithLogFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final String filename = "testBtree6-b.logdb";
        final File file = tempDirectory.resolve(filename).toFile();
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(file, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.put(10, 10);
            bTree.put(5, 5);

            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(file))
        {
            assertEquals(1, readBTree.get(1));
            assertEquals(10, readBTree.get(10));
            assertEquals(5, readBTree.get(5));

            assertEquals(KEY_NOT_FOUND_VALUE, readBTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDBWithLog()
    {
        final String filename = "testBtree6.logdb";
        final File file = tempDirectory.resolve(filename).toFile();
        final long key = 123L;
        final int maxVersions = 100;
        final long nonExistingKey = 964L;

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(file))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
                originalBTree.commit();
            }

            assertEquals(KEY_NOT_FOUND_VALUE, originalBTree.get(nonExistingKey));
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(file))
        {
            for (int i = 0; i < maxVersions; i++)
            {
                assertEquals(i, loadedBTree.get(key, i));
            }

            assertEquals(KEY_NOT_FOUND_VALUE, loadedBTree.get(nonExistingKey));
        }
    }

    @Test
    void shouldBeABleToCommitMultipleTimesWithLog()
    {
        final String filename = "testBtree7.logdb";
        final File file = tempDirectory.resolve(filename).toFile();
        final int numberOfPairs = 600;

        final String original;
        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(file))
        {

            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }

            original = originalBTree.print();
            originalBTree.commit();
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(file))
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
    void shouldBeABleToLoadAndContinuePersistingBtreeWithLog()
    {
        final String filename = "testBtree8.logdb";
        final File file = tempDirectory.resolve(filename).toFile();
        final int numberOfPairs = 300;

        //create new tree and insert elements
        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(file))
        {
            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }
            originalBTree.commit();
        }

        //open the persisted tree and continue inserting elements
        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(file))
        {
            for (long i = numberOfPairs; i < numberOfPairs * 2; i++)
            {
                loadedBTree.put(i, i);
                loadedBTree.commit();
            }
        }

        //open the persisted tree and read all the elements
        try (final BTreeWithLog loadedBTree2 = loadPersistedLogBtree(file))
        {
            for (int i = 0; i < numberOfPairs * 2; i++)
            {
                assertEquals(i, loadedBTree2.get(i));
            }
        }
    }
}
