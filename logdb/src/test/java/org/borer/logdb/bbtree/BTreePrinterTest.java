package org.borer.logdb.bbtree;

import org.borer.logdb.support.TestUtils;
import org.junit.jupiter.api.Test;

import static org.borer.logdb.support.TestUtils.createLeafNodeWithKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreePrinterTest
{
    @Test
    void shouldPrintLeafNode()
    {
        final BTreeNodeLeaf bTreeLeaf = TestUtils.createLeafNodeWithKeys(0, 0);

        for (int i = 0; i < 10; i++)
        {
            bTreeLeaf.insert(i, i);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"0\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "}\n";

        assertEquals(expectedDotString, BTreePrinter.print(bTreeLeaf));
    }

    @Test
    void shouldPrintNonLeafNode()
    {
        final int numKeysPerChild = 5;
        final BTreeNode lastChild = createLeafNodeWithKeys(numKeysPerChild, numKeysPerChild * 10);
        final BTreeNodeNonLeaf bTreeNonLeaf = TestUtils.createNonLeafNodeWithChild(lastChild, 999);

        for (int i = 0; i < 10; i++)
        {
            final int keyStart = numKeysPerChild * i;
            final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, keyStart);

            bTreeNonLeaf.insertChild(i, keyStart, child);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"999\"[label = \" <0> |0|  <5> |5|  <10> |10|  <15> |15|  <20> |20|  <25> |25|  <30> |30|  <35> |35|  <40> |40|  <45> |45|  <lastChild> |Ls \"];\n" +
                "\"999\":0 -> \"0\"\n" +
                "\"999\":5 -> \"5\"\n" +
                "\"999\":10 -> \"10\"\n" +
                "\"999\":15 -> \"15\"\n" +
                "\"999\":20 -> \"20\"\n" +
                "\"999\":25 -> \"25\"\n" +
                "\"999\":30 -> \"30\"\n" +
                "\"999\":35 -> \"35\"\n" +
                "\"999\":40 -> \"40\"\n" +
                "\"999\":45 -> \"45\"\n" +
                "\"999\":lastChild -> \"50\"\n" +
                "\"0\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"5\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"10\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"15\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"20\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"25\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"30\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"35\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"40\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"45\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"50\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "}\n";

        assertEquals(expectedDotString, BTreePrinter.print(bTreeNonLeaf));
    }
}