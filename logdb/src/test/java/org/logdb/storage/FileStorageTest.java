package org.logdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTreeNodeLeaf;
import org.logdb.bbtree.BTreeNodeNonLeaf;
import org.logdb.bbtree.IdSupplier;
import org.logdb.bbtree.NodesManager;
import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryCopy;
import org.logdb.bit.MemoryFactory;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.logdb.storage.FileStorageFactory.createNew;
import static org.logdb.support.TestUtils.BYTE_ORDER;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class FileStorageTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageTest.class);

    @TempDir Path tempDirectory;

    @Test
    void shouldNotBeAbleToCreateFileStorageWithInvalidPageSize() throws IOException
    {
        try (FileStorage ignored = createNew(tempDirectory, FileType.INDEX, TestUtils.SEGMENT_FILE_SIZE, BYTE_ORDER, 100))
        {
            fail("should have failed creating file storage with invalid page size");
        }
        catch (final IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Page Size must be bigger than 128 bytes and a power of 2. Provided was " + 100);
        }

        try(FileStorage ignored = createNew(tempDirectory, FileType.INDEX, TestUtils.SEGMENT_FILE_SIZE, BYTE_ORDER, 4097))
        {
            fail("should have failed creating file storage with invalid page size");
        }
        catch (final IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Page Size must be bigger than 128 bytes and a power of 2. Provided was " + 4097);
        }
    }

    @Test
    void shouldPersistAndLoadLeafNode() throws IOException
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));
        final long timestamp = 1L;
        final long version = 2L;
        final int previousRootPageNumber = 1234;

        try (FileStorage storage = createNew(tempDirectory, FileType.INDEX, TestUtils.SEGMENT_FILE_SIZE, BYTE_ORDER, PAGE_SIZE_BYTES))
        {
            final NodesManager nodesManager = new NodesManager(storage);

            final long pageNumber = leaf.commit(nodesManager, true, previousRootPageNumber, timestamp, version);
            storage.flush();

            final DirectMemory persistedMemory = MemoryFactory.allocateDirect(PAGE_SIZE_BYTES, BYTE_ORDER);
            storage.mapPage(pageNumber, persistedMemory);

            final HeapMemory heapMemory = storage.allocateHeapPage();
            MemoryCopy.copy(persistedMemory, heapMemory);
            final BTreeNodeLeaf loadedLeaf = BTreeNodeLeaf.load(pageNumber, heapMemory);

            assertTrue(loadedLeaf.isRoot());
            assertEquals(previousRootPageNumber, loadedLeaf.getPreviousRoot());
            assertEquals(version, loadedLeaf.getVersion());
            assertEquals(timestamp, loadedLeaf.getTimestamp());

            for (int i = 0; i < numKeys; i++)
            {
                assertEquals(i, loadedLeaf.get(i));
            }
        }
    }

    @Test
    void shouldPersistAndLoadLeafNonNode() throws IOException
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));
        final BTreeNodeNonLeaf nonLeaf = TestUtils.createNonLeafNodeWithChild(leaf);
        final long timestamp = 2L;
        final long version = 1L;
        final int previousRootPageNumber = 56789;

        try (FileStorage storage = createNew(tempDirectory, FileType.INDEX, TestUtils.SEGMENT_FILE_SIZE, BYTE_ORDER, PAGE_SIZE_BYTES))
        {
            final NodesManager nodesManager = new NodesManager(storage);

            final long pageNumber = nonLeaf.commit(nodesManager, true, previousRootPageNumber, timestamp, version);
            storage.flush();

            final DirectMemory persistedMemory = MemoryFactory.allocateDirect(PAGE_SIZE_BYTES, BYTE_ORDER);
            storage.mapPage(pageNumber, persistedMemory);

            final HeapMemory heapMemory = storage.allocateHeapPage();
            MemoryCopy.copy(persistedMemory, heapMemory);
            final BTreeNodeNonLeaf loadedNonLeaf = BTreeNodeNonLeaf.load(pageNumber, heapMemory);

            assertTrue(loadedNonLeaf.isRoot());
            assertEquals(previousRootPageNumber, loadedNonLeaf.getPreviousRoot());
            assertEquals(version, loadedNonLeaf.getVersion());
            assertEquals(timestamp, loadedNonLeaf.getTimestamp());

            final long pageNumberLeaf = loadedNonLeaf.getValue(0);
            final DirectMemory persistedMemoryLeaf = MemoryFactory.allocateDirect(PAGE_SIZE_BYTES, BYTE_ORDER);
            storage.mapPage(pageNumberLeaf, persistedMemoryLeaf);

            final HeapMemory heapMemoryLeaf = storage.allocateHeapPage();
            MemoryCopy.copy(persistedMemoryLeaf, heapMemoryLeaf);
            final BTreeNodeLeaf loadedLeaf = BTreeNodeLeaf.load(pageNumber, heapMemoryLeaf);

            assertFalse(loadedLeaf.isRoot());

            for (int i = 0; i < numKeys; i++)
            {
                assertEquals(i, loadedLeaf.get(i));
            }
        }
    }

    @Test
    void shouldRollOverWhenAppendingPageAlignedAndFileIsFull() throws IOException
    {
        final int segmentFileSize = 256;
        final int pageSizeBytes = 128;

        final byte[] record = new byte[pageSizeBytes];
        Arrays.fill(record, (byte) 1);
        final ByteBuffer buffer = ByteBuffer.wrap(record);

        assertEquals(0, Files.list(tempDirectory).count());

        try(FileStorage fileStorage = createNew(tempDirectory, FileType.HEAP, segmentFileSize, BYTE_ORDER, pageSizeBytes))
        {
            final List<Path> storageFiles = Files.list(tempDirectory).collect(Collectors.toList());
            assertEquals(1, storageFiles.size());
            assertEquals("0-heap.logdb", storageFiles.get(0).getFileName().toString());

            fileStorage.appendPageAligned(buffer);
            fileStorage.appendPageAligned(buffer);

            final List<Path> storageFilesAfterRoll = Files.list(tempDirectory)
                    .sorted(FileType.HEAP)
                    .collect(Collectors.toList());
            assertEquals(2, storageFilesAfterRoll.size());
            assertEquals("0-heap.logdb", storageFilesAfterRoll.get(0).getFileName().toString());
            assertEquals("1-heap.logdb", storageFilesAfterRoll.get(1).getFileName().toString());
        }
    }

    @Test
    void shouldRollOverWhenAppendingAndFileIsFull() throws IOException
    {
        final int segmentFileSize = 256;
        final int pageSizeBytes = 128;

        final byte[] record = new byte[pageSizeBytes];
        Arrays.fill(record, (byte) 1);
        final ByteBuffer buffer = ByteBuffer.wrap(record);

        assertEquals(0, Files.list(tempDirectory).count());

        try(FileStorage fileStorage = createNew(tempDirectory, FileType.INDEX, segmentFileSize, BYTE_ORDER, pageSizeBytes))
        {
            final List<Path> storageFiles = Files.list(tempDirectory).collect(Collectors.toList());
            assertEquals(1, storageFiles.size());
            assertEquals("0-index.logdbIndex", storageFiles.get(0).getFileName().toString());

            fileStorage.append(buffer);
            fileStorage.append(buffer);

            final List<Path> storageFilesAfterRoll = Files.list(tempDirectory)
                    .sorted(FileType.INDEX)
                    .collect(Collectors.toList());
            assertEquals(2, storageFilesAfterRoll.size());
            assertEquals("0-index.logdbIndex", storageFilesAfterRoll.get(0).getFileName().toString());
            assertEquals("1-index.logdbIndex", storageFilesAfterRoll.get(1).getFileName().toString());
        }
    }
}