package org.borer.logdb.integration;

import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.bbtree.BTreeImpl;
import org.borer.logdb.bbtree.BTreeWithLog;
import org.borer.logdb.storage.FileStorage;
import org.borer.logdb.storage.NodesManager;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.borer.logdb.support.TestUtils.PAGE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BBTreeIntergrationTest
{
    @TempDir
    Path tempDirectory;

    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNode()
    {
        final String filename = "testBtree1.logdb";
        final BTree bTree = createNewPersistedBtree(filename);

        bTree.put(1, 1);
        bTree.put(10, 10);
        bTree.put(5, 5);
        bTree.commit();

        bTree.close();

        final BTree readBTree = loadPersistedBtree(filename);

        assertEquals(1, readBTree.get(1));
        assertEquals(10, readBTree.get(10));
        assertEquals(5, readBTree.get(5));

        readBTree.close();
    }

    @Test
    void shouldBeABleToPersistAndReadABBtree()
    {
        final String filename = "testBtree2.logdb";
        final BTree originalBTree = createNewPersistedBtree(filename);

        final int numKeys = 100;
        for (int i = 0; i < numKeys; i++)
        {
            originalBTree.put(i, i);
        }

        originalBTree.commit();
        originalBTree.close();

        final BTree loadedBTree = loadPersistedBtree(filename);

        final String loadedStructure = loadedBTree.print();
        final String expectedStructure = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"179\"[label = \" <28> |14|  <57> |28|  <86> |42|  <115> |56|  <144> |70|  <173> |84|  <lastChild> |Ls \"];\n" +
                "\"179\":28 -> \"28\"\n" +
                "\"179\":57 -> \"57\"\n" +
                "\"179\":86 -> \"86\"\n" +
                "\"179\":115 -> \"115\"\n" +
                "\"179\":144 -> \"144\"\n" +
                "\"179\":173 -> \"173\"\n" +
                "\"179\":lastChild -> \"178\"\n" +
                "\"28\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11|  <12> |12|  <13> |13| \"];\n" +
                "\"57\"[label = \" <14> |14|  <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24|  <25> |25|  <26> |26|  <27> |27| \"];\n" +
                "\"86\"[label = \" <28> |28|  <29> |29|  <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"115\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54|  <55> |55| \"];\n" +
                "\"144\"[label = \" <56> |56|  <57> |57|  <58> |58|  <59> |59|  <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"173\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"178\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89|  <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "}\n";

        assertEquals(expectedStructure, loadedStructure);

        for (int i = 0; i < numKeys; i++)
        {
            assertEquals(i, loadedBTree.get(i));
        }

        loadedBTree.close();
    }

    @Test
    void shouldBeABleToCommitMultipleTimes()
    {
        final String filename = "testBtree3.logdb";
        final BTree originalBTree = createNewPersistedBtree(filename);

        final List<Long> expectedOrder = new ArrayList<>();
        final int numberOfPairs = 100;
        for (long i = 0; i < numberOfPairs; i++)
        {
            expectedOrder.add(i);
            originalBTree.put(i, i);
            originalBTree.commit();
        }

        originalBTree.commit();
        originalBTree.close();

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        final BTreeImpl loadedBTree = loadPersistedBtree(filename);

        loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }

        loadedBTree.close();
    }

    @Test
    void shouldConsumeKeyValuesInOrderAfterCommit()
    {
        final String filename = "testBtree4.logdb";
        final BTree originalBTree = createNewPersistedBtree(filename);

        final List<Long> expectedOrder = new ArrayList<>();
        for (long i = 0; i < 100; i++)
        {
            expectedOrder.add(i);
            originalBTree.put(i, i);
        }

        originalBTree.commit();
        originalBTree.close();

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        final BTreeImpl loadedBTree = loadPersistedBtree(filename);

        loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }

        loadedBTree.close();
    }

    @Test
    void shouldGetHistoricValuesFromOpenDB()
    {
        final String filename = "testBtree5.logdb";
        final BTree originalBTree = createNewPersistedBtree(filename);

        final long key = 123L;
        final int maxVersions = 100;
        for (long i = 0; i < maxVersions; i++)
        {
            originalBTree.put(key, i);
        }

        originalBTree.commit();
        originalBTree.close();

        final BTreeImpl loadedBTree = loadPersistedBtree(filename);

        final int[] index = new int[1]; //ugh... lambdas
        for (index[0] = 0; index[0] < maxVersions; index[0]++)
        {
            loadedBTree.consumeAll(index[0], (k, value) ->
            {
                assertEquals(key, k);
                assertEquals(index[0], value);
            });
        }

        loadedBTree.close();
    }

    @Test
    void shouldBeABleToCommitMultipleTimesWithLog()
    {
        final String filename = "testBtree6.logdb";
        final BTreeWithLog originalBTree = createNewPersistedLogBtree(filename);

        final int numberOfPairs = 600;
        for (long i = 0; i < numberOfPairs; i++)
        {
            originalBTree.put(i, i);
            originalBTree.commit();
        }

        final String original = originalBTree.print();

        originalBTree.commit();
        originalBTree.close();

        final BTreeWithLog loadedBTree = loadPersistedLogBtree(filename);

        final String loaded = loadedBTree.print();

        assertEquals(original, loaded);

        for (int i = 0; i < numberOfPairs; i++)
        {
            assertEquals(i, loadedBTree.get(i));
        }

        loadedBTree.close();
    }

    private BTreeWithLog createNewPersistedLogBtree(final String filename)
    {
        final FileStorage storage = FileStorage.createNewFileDb(
                tempDirectory.resolve(filename).toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeWithLog(nodesManage);
    }

    private BTreeWithLog loadPersistedLogBtree(final String filename)
    {
        final FileStorage storage = FileStorage.openDbFile(
                tempDirectory.resolve(filename).toFile());

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeWithLog(nodesManager);
    }

    private BTreeImpl createNewPersistedBtree(final String filename)
    {
        final FileStorage storage = FileStorage.createNewFileDb(
                tempDirectory.resolve(filename).toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeImpl(nodesManage);
    }

    private BTreeImpl loadPersistedBtree(final String filename)
    {
        final FileStorage storage = FileStorage.openDbFile(
                tempDirectory.resolve(filename).toFile());

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeImpl(nodesManager);
    }
}
