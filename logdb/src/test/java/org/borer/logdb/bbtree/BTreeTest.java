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
                "\"14\"[label = \" <30> |30|  <60> |60|  <lastChild> |Ls \"];\n" +
                "\"14\":30 -> \"2\"\n" +
                "\"14\":60 -> \"13\"\n" +
                "\"14\":lastChild -> \"21\"\n" +
                "\"2\"[label = \" <5> |5|  <10> |10|  <15> |15|  <20> |20|  <25> |25|  <lastChild> |Ls \"];\n" +
                "\"2\":5 -> \"0\"\n" +
                "\"2\":10 -> \"1\"\n" +
                "\"2\":15 -> \"3\"\n" +
                "\"2\":20 -> \"4\"\n" +
                "\"2\":25 -> \"5\"\n" +
                "\"2\":lastChild -> \"6\"\n" +
                "\"0\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"1\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"3\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"4\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"5\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"6\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"13\"[label = \" <35> |35|  <40> |40|  <45> |45|  <50> |50|  <55> |55|  <lastChild> |Ls \"];\n" +
                "\"13\":35 -> \"7\"\n" +
                "\"13\":40 -> \"8\"\n" +
                "\"13\":45 -> \"9\"\n" +
                "\"13\":50 -> \"10\"\n" +
                "\"13\":55 -> \"11\"\n" +
                "\"13\":lastChild -> \"12\"\n" +
                "\"7\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"8\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"9\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"10\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"11\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "\"12\"[label = \" <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"21\"[label = \" <65> |65|  <70> |70|  <75> |75|  <80> |80|  <85> |85|  <90> |90|  <lastChild> |Ls \"];\n" +
                "\"21\":65 -> \"15\"\n" +
                "\"21\":70 -> \"16\"\n" +
                "\"21\":75 -> \"17\"\n" +
                "\"21\":80 -> \"18\"\n" +
                "\"21\":85 -> \"19\"\n" +
                "\"21\":90 -> \"20\"\n" +
                "\"21\":lastChild -> \"22\"\n" +
                "\"15\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"16\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"17\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"18\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                "\"19\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84| \"];\n" +
                "\"20\"[label = \" <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"22\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "}\n";

        assertEquals(expectedTree, BTreePrinter.print(bTree, nodesManager));
    }
}