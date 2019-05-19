package org.logdb.storage;

import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreePrinter;
import org.logdb.support.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class MemoryStorageTest
{
//    @Test
    void shouldBeAbleToReadBtreeAfterCommit()
    {
        final Storage memoryStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(memoryStorage);
        final BTreeImpl originalBTree = new BTreeImpl(nodesManager);

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        originalBTree.commit();

        //load btree from existing memory
        final NodesManager readNodesManager = new NodesManager(memoryStorage);
        final BTreeImpl loadedBTree = new BTreeImpl(readNodesManager);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}