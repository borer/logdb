package org.borer.logdb.integration;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.storage.FileStorage;
import org.borer.logdb.storage.NodesManager;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
                "\"-1\"[label = \" <122> |30|  <219> |60|  <lastChild> |Ls \"];\n" +
                "\"-1\":122 -> \"122\"\n" +
                "\"-1\":219 -> \"219\"\n" +
                "\"-1\":lastChild -> \"250\"\n" +
                "\"122\"[label = \" <12> |5|  <23> |10|  <34> |15|  <45> |20|  <56> |25|  <lastChild> |Ls \"];\n" +
                "\"122\":12 -> \"12\"\n" +
                "\"122\":23 -> \"23\"\n" +
                "\"122\":34 -> \"34\"\n" +
                "\"122\":45 -> \"45\"\n" +
                "\"122\":56 -> \"56\"\n" +
                "\"122\":lastChild -> \"67\"\n" +
                "\"12\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"23\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"34\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"45\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"56\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"67\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"219\"[label = \" <78> |35|  <89> |40|  <100> |45|  <111> |50|  <123> |55|  <lastChild> |Ls \"];\n" +
                "\"219\":78 -> \"78\"\n" +
                "\"219\":89 -> \"89\"\n" +
                "\"219\":100 -> \"100\"\n" +
                "\"219\":111 -> \"111\"\n" +
                "\"219\":123 -> \"123\"\n" +
                "\"219\":lastChild -> \"139\"\n" +
                "\"78\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"89\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"100\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"111\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"123\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "\"139\"[label = \" <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"250\"[label = \" <155> |65|  <171> |70|  <187> |75|  <203> |80|  <220> |85|  <236> |90|  <lastChild> |Ls \"];\n" +
                "\"250\":155 -> \"155\"\n" +
                "\"250\":171 -> \"171\"\n" +
                "\"250\":187 -> \"187\"\n" +
                "\"250\":203 -> \"203\"\n" +
                "\"250\":220 -> \"220\"\n" +
                "\"250\":236 -> \"236\"\n" +
                "\"250\":lastChild -> \"249\"\n" +
                "\"155\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"171\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"187\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"203\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                "\"220\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84| \"];\n" +
                "\"236\"[label = \" <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"249\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
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
        final BTree loadedBTree = loadPersistedBtree(filename);

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
        final BTree loadedBTree = loadPersistedBtree(filename);

        loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }

        loadedBTree.close();
    }

    private BTree createNewPersistedBtree(final String filename)
    {
        final FileStorage storage = FileStorage.createNewFileDb(
                tempDirectory.resolve(filename).toFile(),
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                Config.PAGE_SIZE_BYTES);

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTree(nodesManage);
    }

    private BTree loadPersistedBtree(final String filename)
    {
        final FileStorage storage = FileStorage.openDbFile(
                tempDirectory.resolve(filename).toFile());

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTree(nodesManager);
    }
}
