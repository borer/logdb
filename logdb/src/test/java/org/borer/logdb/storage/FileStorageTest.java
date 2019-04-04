package org.borer.logdb.storage;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.bbtree.BTreeNodeLeaf;
import org.borer.logdb.bbtree.BTreeNodeNonLeaf;
import org.borer.logdb.bbtree.BTreePrinter;
import org.borer.logdb.bbtree.IdSupplier;
import org.borer.logdb.bit.Memory;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageTest
{
    private static final String DB_FILENAME = "test.logdb";

    private FileStorage storage;
    private NodesManager nodesManager;

    @BeforeEach
    void setUp()
    {
        final File file = new File(DB_FILENAME);
        file.delete();

        storage = FileStorage.createNewFileDb(
                DB_FILENAME,
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                Config.PAGE_SIZE_BYTES);

        nodesManager = new NodesManager(storage);
    }

    @Test
    void shouldPersistAndLoadLeafNode()
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));

        final long pageNumber = leaf.commit(nodesManager);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        leaf.updateBuffer(persistedMemory);

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, leaf.get(i));
        }
    }

    @Test
    void shouldPersistAndLoadLeafNonNode()
    {
        final int numKeys = 10;
        final BTreeNodeLeaf leaf = TestUtils.createLeafNodeWithKeys(numKeys, 0, new IdSupplier(0));
        final BTreeNodeNonLeaf nonLeaf = TestUtils.createNonLeafNodeWithChild(leaf);

        final long pageNumber = nonLeaf.commit(nodesManager);
        storage.flush();

        final Memory persistedMemory = storage.loadPage(pageNumber);
        nonLeaf.updateBuffer(persistedMemory);

        final long pageNumberLeaf = nonLeaf.getValue(0);
        final Memory persistedMemoryLeaf = storage.loadPage(pageNumberLeaf);
        leaf.updateBuffer(persistedMemoryLeaf);

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, leaf.get(i));
        }
    }

    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNode() throws IOException
    {
        final NodesManager nodesManager = new NodesManager(storage);
        final BTree bTree = new BTree(nodesManager);

        bTree.put(1, 1);
        bTree.put(10, 10);
        bTree.put(5, 5);
        bTree.commit();

        storage.close();

        //read btree from file
        final FileStorage oldFileDB = FileStorage.openDbFile(DB_FILENAME);

        final NodesManager readNodesManager = new NodesManager(oldFileDB);
        final BTree readBTree = new BTree(readNodesManager);

        assertEquals(1, readBTree.get(1));
        assertEquals(10, readBTree.get(10));
        assertEquals(5, readBTree.get(5));
    }

//    @Test
    void shouldBeABleToPersistAndReadABBtree() throws IOException
    {
        final NodesManager nodesManager = new NodesManager(storage);
        final BTree originalBTree = new BTree(nodesManager);

        for (int i = 0; i < 100; i++)
        {
            originalBTree.put(i, i);
        }

        final String originalBtreePrint = BTreePrinter.print(originalBTree, nodesManager);

        originalBTree.commit();

        storage.close();

        //read btree from file
        final FileStorage oldFileDB = FileStorage.openDbFile(DB_FILENAME);

        final NodesManager readNodesManager = new NodesManager(oldFileDB);
        final BTree loadedBTree = new BTree(readNodesManager);

        final String loadedBtreePrint = BTreePrinter.print(loadedBTree, readNodesManager);

        assertEquals(originalBtreePrint, loadedBtreePrint);
    }
}