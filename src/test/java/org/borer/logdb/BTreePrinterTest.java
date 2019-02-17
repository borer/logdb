package org.borer.logdb;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createLeafNodeWithKeys;
import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreePrinterTest
{
    @Test
    void shouldPrintLeafNode()
    {
        final BTreeNodeLeaf bTreeLeaf = new BTreeNodeLeaf();

        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer key = createValue("key" + i);
            final ByteBuffer value = createValue("value" + i);

            bTreeLeaf.insert(key, value);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"0\"[label = \" <key0> |value0|  <key1> |value1|  <key2> |value2|  <key3> |value3|  <key4> |value4|  <key5> |value5|  <key6> |value6|  <key7> |value7|  <key8> |value8|  <key9> |value9| \"];\n" +
                "}\n";

        assertEquals(expectedDotString, BTreePrinter.print(bTreeLeaf));
    }

    @Test
    void shouldPrintNonLeafNode()
    {
        final BTreeNodeNonLeaf bTreeNonLeaf = new BTreeNodeNonLeaf();

        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final int keyStart = numKeysPerChild * i;
            final ByteBuffer childKeyBuffer = createValue("key" + keyStart);
            final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, keyStart);

            bTreeNonLeaf.insertChild(i, childKeyBuffer, child);
        }

        final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, numKeysPerChild * 10);
        bTreeNonLeaf.setChild(10, child);

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"0\"[label = \" <key0> |key0|  <key5> |key5|  <key10> |key10|  <key15> |key15|  <key20> |key20|  <key25> |key25|  <key30> |key30|  <key35> |key35|  <key40> |key40|  <key45> |key45|  <lastChild> |Ls \"];\n" +
                "\"0\":key0 -> \"0\"\n" +
                "\"0\":key5 -> \"0\"\n" +
                "\"0\":key10 -> \"0\"\n" +
                "\"0\":key15 -> \"0\"\n" +
                "\"0\":key20 -> \"0\"\n" +
                "\"0\":key25 -> \"0\"\n" +
                "\"0\":key30 -> \"0\"\n" +
                "\"0\":key35 -> \"0\"\n" +
                "\"0\":key40 -> \"0\"\n" +
                "\"0\":key45 -> \"0\"\n" +
                "\"0\":lastChild -> \"0\"\n" +
                "\"0\"[label = \" <key0> |value0|  <key1> |value1|  <key2> |value2|  <key3> |value3|  <key4> |value4| \"];\n" +
                "\"0\"[label = \" <key5> |value5|  <key6> |value6|  <key7> |value7|  <key8> |value8|  <key9> |value9| \"];\n" +
                "\"0\"[label = \" <key10> |value10|  <key11> |value11|  <key12> |value12|  <key13> |value13|  <key14> |value14| \"];\n" +
                "\"0\"[label = \" <key15> |value15|  <key16> |value16|  <key17> |value17|  <key18> |value18|  <key19> |value19| \"];\n" +
                "\"0\"[label = \" <key20> |value20|  <key21> |value21|  <key22> |value22|  <key23> |value23|  <key24> |value24| \"];\n" +
                "\"0\"[label = \" <key25> |value25|  <key26> |value26|  <key27> |value27|  <key28> |value28|  <key29> |value29| \"];\n" +
                "\"0\"[label = \" <key30> |value30|  <key31> |value31|  <key32> |value32|  <key33> |value33|  <key34> |value34| \"];\n" +
                "\"0\"[label = \" <key35> |value35|  <key36> |value36|  <key37> |value37|  <key38> |value38|  <key39> |value39| \"];\n" +
                "\"0\"[label = \" <key40> |value40|  <key41> |value41|  <key42> |value42|  <key43> |value43|  <key44> |value44| \"];\n" +
                "\"0\"[label = \" <key45> |value45|  <key46> |value46|  <key47> |value47|  <key48> |value48|  <key49> |value49| \"];\n" +
                "\"0\"[label = \" <key50> |value50|  <key51> |value51|  <key52> |value52|  <key53> |value53|  <key54> |value54| \"];\n" +
                "}\n";

        assertEquals(expectedDotString, BTreePrinter.print(bTreeNonLeaf));
    }
}