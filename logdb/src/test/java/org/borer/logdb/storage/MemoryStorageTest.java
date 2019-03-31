package org.borer.logdb.storage;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.bbtree.BTreePrinter;
import org.borer.logdb.support.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemoryStorageTest
{
//    @Test
    void shouldBeAbleToReadBtreeAfterCommit()
    {
        final Storage memoryStorage = new MemoryStorage(TestUtils.BYTE_ORDER, Config.PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(memoryStorage);
        final BTree originalBTree = new BTree(nodesManager);

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        originalBTree.commit();

        //load btree from existing memory
        final NodesManager readNodesManager = new NodesManager(memoryStorage);
        final BTree loadedBTree = new BTree(readNodesManager);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}