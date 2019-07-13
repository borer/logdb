package org.logdb.storage.memory;

import org.junit.jupiter.api.Test;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreePrinter;
import org.logdb.bbtree.NodesManager;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.logdb.bbtree.BTreeValidation.isNewTree;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;
import static org.logdb.support.TestUtils.createInitialRootReference;
import static org.logdb.support.TestUtils.createRootIndex;

class MemoryStorageTest
{
    @Test
    void shouldBeAbleToReadBtreeAfterCommit() throws IOException
    {
        final Storage memoryStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE_BYTES, MEMORY_CHUNK_SIZE);
        final RootIndex rootIndex = createRootIndex(PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(memoryStorage, rootIndex);
        final BTreeImpl originalBTree = new BTreeImpl(
                nodesManager,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager));

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        originalBTree.commit();

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        //load btree from existing memory
        final NodesManager readNodesManager = new NodesManager(memoryStorage, rootIndex);
        final @PageNumber long pageNumber = readNodesManager.loadLastRootPageNumber();
        assertFalse(isNewTree(pageNumber));

        final BTreeImpl loadedBTree = new BTreeImpl(
                readNodesManager,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                pageNumber,
                null);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}