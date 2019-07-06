package org.logdb.storage;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreePrinter;
import org.logdb.bbtree.NodesManager;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.logdb.bbtree.BTreeValidation.isNewTree;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;
import static org.logdb.support.TestUtils.createInitialRootReference;

class MemoryStorageTest
{
    @Test
    @Disabled
    void shouldBeAbleToReadBtreeAfterCommit() throws IOException
    {
        final Storage memoryStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(memoryStorage);
        final BTreeImpl originalBTree = new BTreeImpl(
                nodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager));

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        originalBTree.commit();

        //load btree from existing memory
        final NodesManager readNodesManager = new NodesManager(memoryStorage);
        final @PageNumber long pageNumber = readNodesManager.loadLastRootPageNumber();
        assertFalse(isNewTree(pageNumber));

        final BTreeImpl loadedBTree = new BTreeImpl(
                readNodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                pageNumber,
                null);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}