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
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class MemoryStorageTest
{
    @Test
    @Disabled
    void shouldBeAbleToReadBtreeAfterCommit() throws IOException
    {
        final Storage memoryStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(memoryStorage);
        final BTreeImpl originalBTree = new BTreeImpl(nodesManager, new StubTimeSource(), INITIAL_VERSION);

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        originalBTree.commit();

        //load btree from existing memory
        final NodesManager readNodesManager = new NodesManager(memoryStorage);
        final BTreeImpl loadedBTree = new BTreeImpl(readNodesManager, new StubTimeSource(), INITIAL_VERSION);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}