package org.borer.logdb.bbtree;

import org.borer.logdb.Config;
import org.borer.logdb.storage.MemoryStorage;
import org.borer.logdb.storage.NodesManager;
import org.borer.logdb.storage.Storage;
import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreeTest
{
    private BTree bTree;
    private NodesManager nodesManager;

    @BeforeEach
    void setUp()
    {
        final Storage storage = new MemoryStorage(TestUtils.BYTE_ORDER, Config.PAGE_SIZE_BYTES);

        nodesManager = new NodesManager(storage);
        bTree = new BTree(nodesManager);
    }

    @Test
    void shouldBeAbleToGetElementsFromPast()
    {
        final long key = 5L;
        final long expectedValue1 = 1L;
        final long expectedValue2 = 2L;
        final long expectedValue3 = 3L;

        bTree.put(key, expectedValue1);
        bTree.put(key, expectedValue2);
        bTree.put(key, expectedValue3);

        final long actual0 = bTree.get(key, 0);
        assertEquals(-1, actual0);

        //index is 0 based, but the version 0 is from the constructor
        final long actual1 = bTree.get(key, 1);
        assertEquals(expectedValue1, actual1);

        final long actual2 = bTree.get(key, 2);
        assertEquals(expectedValue2, actual2);

        final long actual3 = bTree.get(key, 3);
        assertEquals(expectedValue3, actual3);

        //get latest version by default
        final long actualLatest = bTree.get(key);
        assertEquals(expectedValue3, actualLatest);
    }

    @Test
    void shouldConsumeKeyValuesInOrder()
    {
        final List<Long> expectedOrder = new ArrayList<>();
        for (long i = 0; i < 100; i++)
        {
            expectedOrder.add(i);
            bTree.put(i, i);
        }

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        bTree.consumeAll((key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }
    }

    @Test
    void shouldConsumeKeyValuesInOrderAfterCommit()
    {
        final List<Long> expectedOrder = new ArrayList<>();
        for (long i = 0; i < 100; i++)
        {
            expectedOrder.add(i);
            bTree.put(i, i);
        }

        bTree.commit();

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        bTree.consumeAll((key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }
    }

    @Test
    void shouldConsumeKeyValuesInOrderForAGivenVersion()
    {
        final List<Long> expectedOrder = new ArrayList<>();
        final int version = 50;
        for (long i = 0; i < 100; i++)
        {
            if (i < version)
            {
                expectedOrder.add(i);
            }

            bTree.put(i, i);
        }

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        bTree.consumeAll(version, (key, value) -> actualOrder.addLast(key));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }
    }

    @Test
    void shouldBeABleToRemoveKeysFromTree()
    {
        for (int i = 0; i < 100; i++)
        {
            bTree.put(i, i);
        }

        for (int i = 0; i < 100; i++)
        {
            bTree.remove(i);
        }

        assertEquals(1L, bTree.getNodesCount());
    }

    @Test
    void shouldBeAbleToInsert100Elements()
    {
        for (int i = 0; i < 100; i++)
        {
            bTree.put(i, i);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"250\"[label = \" <30> |30|  <60> |60|  <lastChild> |Ls \"];\n" +
                "\"250\":30 -> \"123\"\n" +
                "\"250\":60 -> \"220\"\n" +
                "\"250\":lastChild -> \"249\"\n" +
                "\"123\"[label = \" <5> |5|  <10> |10|  <15> |15|  <20> |20|  <25> |25|  <lastChild> |Ls \"];\n" +
                "\"123\":5 -> \"11\"\n" +
                "\"123\":10 -> \"22\"\n" +
                "\"123\":15 -> \"33\"\n" +
                "\"123\":20 -> \"44\"\n" +
                "\"123\":25 -> \"55\"\n" +
                "\"123\":lastChild -> \"66\"\n" +
                "\"11\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"22\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"33\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"44\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"55\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"66\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"220\"[label = \" <35> |35|  <40> |40|  <45> |45|  <50> |50|  <55> |55|  <lastChild> |Ls \"];\n" +
                "\"220\":35 -> \"77\"\n" +
                "\"220\":40 -> \"88\"\n" +
                "\"220\":45 -> \"99\"\n" +
                "\"220\":50 -> \"110\"\n" +
                "\"220\":55 -> \"121\"\n" +
                "\"220\":lastChild -> \"138\"\n" +
                "\"77\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"88\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"99\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"110\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"121\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "\"138\"[label = \" <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"249\"[label = \" <65> |65|  <70> |70|  <75> |75|  <80> |80|  <85> |85|  <90> |90|  <lastChild> |Ls \"];\n" +
                "\"249\":65 -> \"154\"\n" +
                "\"249\":70 -> \"170\"\n" +
                "\"249\":75 -> \"186\"\n" +
                "\"249\":80 -> \"202\"\n" +
                "\"249\":85 -> \"218\"\n" +
                "\"249\":90 -> \"235\"\n" +
                "\"249\":lastChild -> \"248\"\n" +
                "\"154\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"170\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"186\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"202\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                "\"218\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84| \"];\n" +
                "\"235\"[label = \" <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"248\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "}\n";

        assertEquals(expectedTree, BTreePrinter.print(bTree, nodesManager));
    }
}