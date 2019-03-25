package org.borer.logdb.storage;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageTest
{
    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNode() throws IOException
    {
        final String filename = "test.logdb";

        final File file = new File(filename);
        file.delete();

        final FileStorage newFileDB = FileStorage.createNewFileDb(
                filename,
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                Config.PAGE_SIZE_BYTES);

        final NodesManager nodesManager = new NodesManager(newFileDB);
        final BTree bTree = new BTree(nodesManager);

        bTree.put(1, 1);
        bTree.put(10, 10);
        bTree.put(5, 5);
        bTree.commit();

        newFileDB.close();

        //read btree from file
        final FileStorage oldFileDB = FileStorage.openDbFile(filename);

        final NodesManager readNodesManager = new NodesManager(oldFileDB);
        final BTree readBTree = new BTree(readNodesManager);

        assertEquals(1, readBTree.get(1));
        assertEquals(10, readBTree.get(10));
        assertEquals(5, readBTree.get(5));
    }
}