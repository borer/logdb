package org.borer.logdb.storage;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTreeNodeLeaf;
import org.borer.logdb.bbtree.BTreeNodeNonLeaf;
import org.borer.logdb.bbtree.IdSupplier;
import org.borer.logdb.bit.Memory;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;

class FileStorageTest
{
    private static final String DB_FILE_CONDITION = "file";
    @TempDir
    Path tempDirectory;

    private static final String DB_FILENAME = "test.logdb";

    private FileStorage storage;
    private NodesManager nodesManager;

    @BeforeEach
    void setUp()
    {
        storage = FileStorage.createNewFileDb(
                tempDirectory.resolve(DB_FILENAME).toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                Config.PAGE_SIZE_BYTES);

        nodesManager = new NodesManager(storage);
    }

    @AfterEach
    void tearDown()
    {
        try
        {
            storage.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    @ResourceLock(value = DB_FILE_CONDITION, mode = READ_WRITE)
    void shouldPersistAndLoadLeafNode()
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));

        final long pageNumber = leaf.commit(nodesManager, true);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        final BTreeNodeLeaf loadedLeaf = new BTreeNodeLeaf(pageNumber, persistedMemory);

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, loadedLeaf.get(i));
        }
    }

    @Test
    @ResourceLock(value = DB_FILE_CONDITION, mode = READ_WRITE)
    void shouldPersistAndLoadLeafNonNode()
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));
        final BTreeNodeNonLeaf nonLeaf = TestUtils.createNonLeafNodeWithChild(leaf);

        final long pageNumber = nonLeaf.commit(nodesManager, true);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        final BTreeNodeNonLeaf loadedNonLeaf = new BTreeNodeNonLeaf(pageNumber, persistedMemory);

        final long pageNumberLeaf = loadedNonLeaf.getValue(0);
        final Memory persistedMemoryLeaf = storage.loadPage(pageNumberLeaf);
        final BTreeNodeLeaf loadedLeaf = new BTreeNodeLeaf(pageNumber, persistedMemoryLeaf);

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, loadedLeaf.get(i));
        }
    }
}