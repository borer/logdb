package org.logdb.bbtree;

import org.junit.jupiter.api.Test;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.TestUtils;
import org.logdb.time.TimeUnits;

import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;
import static org.logdb.support.TestUtils.createLeafNodeWithKeys;

class BTreePrinterTest
{
    @Test
    void shouldPrintLeafNode()
    {
        final BTreeNodeLeaf bTreeLeaf = createLeafNodeWithKeys(0, 0, new IdSupplier(0));

        for (int i = 0; i < 10; i++)
        {
            bTreeLeaf.insert(i, i);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"0\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "}\n";

        assertEquals(expectedDotString, BTreePrinter.print(bTreeLeaf, null));
    }

    @Test
    void shouldPrintNonLeafNode()
    {
        final IdSupplier idSupplier = new IdSupplier(0);
        final int numKeysPerChild = 5;
        final BTreeNodeHeap lastChild = createLeafNodeWithKeys(numKeysPerChild, numKeysPerChild * 10, idSupplier);
        final BTreeNodeNonLeaf bTreeNonLeaf = TestUtils.createNonLeafNodeWithChild(lastChild, 999);

        for (int i = 0; i < 10; i++)
        {
            final int keyStart = numKeysPerChild * i;
            final BTreeNodeHeap child = createLeafNodeWithKeys(numKeysPerChild, keyStart, idSupplier);

            bTreeNonLeaf.insertChild(i, keyStart, child);
        }

        for (int i = 0; i < 5; i++)
        {
            bTreeNonLeaf.insertLog(i, i);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"999\"[label = \"{ log | { 0-0 |  1-1 |  2-2 |  3-3 |  4-4 }}| <1> |0|  <2> |5|  <3> |10|  <4> |15|  <5> |20|  <6> |25|  <7> |30|  <8> |35|  <9> |40|  <10> |45|  <lastChild> |Ls \"];\n" +
                "\"999\":1 -> \"1\"\n" +
                "\"999\":2 -> \"2\"\n" +
                "\"999\":3 -> \"3\"\n" +
                "\"999\":4 -> \"4\"\n" +
                "\"999\":5 -> \"5\"\n" +
                "\"999\":6 -> \"6\"\n" +
                "\"999\":7 -> \"7\"\n" +
                "\"999\":8 -> \"8\"\n" +
                "\"999\":9 -> \"9\"\n" +
                "\"999\":10 -> \"10\"\n" +
                "\"999\":lastChild -> \"0\"\n" +
                "\"1\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"2\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"3\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"4\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"5\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"6\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"7\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"8\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"9\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"10\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"0\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "}\n";

        final MemoryStorage storage = new MemoryStorage(ByteOrder.LITTLE_ENDIAN, 4096, MEMORY_CHUNK_SIZE);
        final RootIndex rootIndex = new RootIndex(
                new MemoryStorage(ByteOrder.LITTLE_ENDIAN, 4096, MEMORY_CHUNK_SIZE),
                INITIAL_VERSION,
                TimeUnits.millis(0),
                StorageUnits.INVALID_OFFSET);
        final NodesManager nodesManager = new NodesManager(storage, rootIndex, false);
        assertEquals(expectedDotString, BTreePrinter.print(bTreeNonLeaf, nodesManager));
    }
}