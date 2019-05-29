package org.logdb.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.logdb.bbtree.BTreeNodeLeaf;
import org.logdb.bbtree.BTreeNodeNonLeaf;
import org.logdb.bbtree.IdSupplier;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.Memory;
import org.logdb.bit.MemoryCopy;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class FileStorageTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageTest.class);

    private static final String DB_FILE_CONDITION = "file";

    @TempDir
    Path tempDirectory;

    private static final String DB_FILENAME = "test.logdb";

    private FileStorage storage;
    private NodesManager nodesManager;

    @BeforeEach
    void setUp()
    {
        final Path tempDbPath = tempDirectory.resolve(DB_FILENAME);

        LOGGER.info("Using temp directory " + tempDbPath.toString());

        storage = FileStorage.createNewFileDb(
                tempDbPath.toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

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
    void shouldNotBeAbleToCreateFileStorageWithInvalidPageSize()
    {
        try
        {
            FileStorage.createNewFileDb(
                    tempDirectory.resolve(DB_FILENAME).toFile(),
                    TestUtils.MAPPED_CHUNK_SIZE,
                    TestUtils.BYTE_ORDER,
                    100);
            fail("should have failed creating file storage with invalid page size");
        }
        catch (final IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Page Size must be bigger than 128 bytes and a power of 2. Provided was " + 100);
        }

        try
        {
            FileStorage.createNewFileDb(
                    tempDirectory.resolve(DB_FILENAME).toFile(),
                    TestUtils.MAPPED_CHUNK_SIZE,
                    TestUtils.BYTE_ORDER,
                    4097);
            fail("should have failed creating file storage with invalid page size");
        }
        catch (final IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Page Size must be bigger than 128 bytes and a power of 2. Provided was " + 4097);
        }
    }

    @Test
    @ResourceLock(value = DB_FILE_CONDITION, mode = READ_WRITE)
    void shouldPersistAndLoadLeafNode()
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));

        final long timestamp = 1L;
        final long version = 2L;
        final int previousRootPageNumber = 1234;
        final long pageNumber = leaf.commit(nodesManager, true, previousRootPageNumber, timestamp, version);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        final HeapMemory heapMemory = storage.allocateHeapMemory();
        MemoryCopy.copy(persistedMemory, heapMemory);
        final BTreeNodeLeaf loadedLeaf = new BTreeNodeLeaf(pageNumber, heapMemory);

        assertTrue(loadedLeaf.isRoot());
        assertEquals(previousRootPageNumber, loadedLeaf.getPreviousRoot());
        assertEquals(version, loadedLeaf.getVersion());
        assertEquals(timestamp, loadedLeaf.getTimestamp());

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

        final long timestamp = 2L;
        final long version = 1L;
        final int previousRootPageNumber = 56789;
        final long pageNumber = nonLeaf.commit(nodesManager, true, previousRootPageNumber, timestamp, version);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        final HeapMemory heapMemory = storage.allocateHeapMemory();
        MemoryCopy.copy(persistedMemory, heapMemory);
        final BTreeNodeNonLeaf loadedNonLeaf = new BTreeNodeNonLeaf(pageNumber, heapMemory);

        assertTrue(loadedNonLeaf.isRoot());
        assertEquals(previousRootPageNumber, loadedNonLeaf.getPreviousRoot());
        assertEquals(version, loadedNonLeaf.getVersion());
        assertEquals(timestamp, loadedNonLeaf.getTimestamp());

        final long pageNumberLeaf = loadedNonLeaf.getValue(0);
        final Memory persistedMemoryLeaf = storage.loadPage(pageNumberLeaf);
        final HeapMemory heapMemoryLeaf = storage.allocateHeapMemory();
        MemoryCopy.copy(persistedMemoryLeaf, heapMemoryLeaf);
        final BTreeNodeLeaf loadedLeaf = new BTreeNodeLeaf(pageNumber, heapMemoryLeaf);

        assertFalse(loadedLeaf.isRoot());

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, loadedLeaf.get(i));
        }
    }
}