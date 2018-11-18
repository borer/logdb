package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreeNodeNonLeafTest
{
    private BTreeNodeNonLeaf bTreeNonLeaf;

    @BeforeEach
    void setUp()
    {
        bTreeNonLeaf = new BTreeNodeNonLeaf();
    }

    @Test
    void shouldBeAbleToInsertChild()
    {
        final BTreeNode bTreeNode = createLeafNodeWithKeys(10, 0);
        final ByteBuffer key = createValue("key0");

        bTreeNonLeaf.insertChild(key, bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }
    }

    @Test
    void shouldPrintNode()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final ByteBuffer childKeyBuffer = createValue("key" + (numKeysPerChild * i));
            final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, i);

            bTreeNonLeaf.insertChild(childKeyBuffer, child);
        }

        final String expectedDotString = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "root[label = \"<key0> |key0|<key10> |key10|<key15> |key15|<key20> |key20|<key25> |key25|<key30> |key30|<key35> |key35|<key40> |key40|<key45> |key45|<key5> |key5|\"];\n" +
                "\"root\":key0 -> \"key0\"\n" +
                "\"root\":key10 -> \"key10\"\n" +
                "\"root\":key15 -> \"key15\"\n" +
                "\"root\":key20 -> \"key20\"\n" +
                "\"root\":key25 -> \"key25\"\n" +
                "\"root\":key30 -> \"key30\"\n" +
                "\"root\":key35 -> \"key35\"\n" +
                "\"root\":key40 -> \"key40\"\n" +
                "\"root\":key45 -> \"key45\"\n" +
                "\"root\":key5 -> \"key5\"\n" +
                "key0[label = \"<key0> |value0|<key1> |value1|<key2> |value2|<key3> |value3|<key4> |value4|\"];\n" +
                "key10[label = \"<key2> |value2|<key3> |value3|<key4> |value4|<key5> |value5|<key6> |value6|\"];\n" +
                "key15[label = \"<key3> |value3|<key4> |value4|<key5> |value5|<key6> |value6|<key7> |value7|\"];\n" +
                "key20[label = \"<key4> |value4|<key5> |value5|<key6> |value6|<key7> |value7|<key8> |value8|\"];\n" +
                "key25[label = \"<key5> |value5|<key6> |value6|<key7> |value7|<key8> |value8|<key9> |value9|\"];\n" +
                "key30[label = \"<key10> |value10|<key6> |value6|<key7> |value7|<key8> |value8|<key9> |value9|\"];\n" +
                "key35[label = \"<key10> |value10|<key11> |value11|<key7> |value7|<key8> |value8|<key9> |value9|\"];\n" +
                "key40[label = \"<key10> |value10|<key11> |value11|<key12> |value12|<key8> |value8|<key9> |value9|\"];\n" +
                "key45[label = \"<key10> |value10|<key11> |value11|<key12> |value12|<key13> |value13|<key9> |value9|\"];\n" +
                "key5[label = \"<key1> |value1|<key2> |value2|<key3> |value3|<key4> |value4|<key5> |value5|\"];\n" +
                "}\n";

        final StringBuilder printer = new StringBuilder();
        bTreeNonLeaf.print(printer, "root");

        assertEquals(expectedDotString, printer.toString());
    }

    @Disabled("currently splitting and searching is broker in the btree nonleaf node")
    @Test
    void shouldBeAbleToSplitInTwoNodes()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final String keyValue = "key" + (numKeysPerChild * i);
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final BTreeNode child = new BTreeNodeLeaf();

            bTreeNonLeaf.insertChild(childKeyBuffer, child);
        }

        bTreeNonLeaf.setChildren(10, new BTreeNodeLeaf());

        final StringBuilder printer2 = new StringBuilder();
        bTreeNonLeaf.print(printer2, "root");
        System.out.println("printing before split");
        System.out.println(printer2.toString());
        System.out.println("end printing");

        //first 5 childs have first 25 keys
        final BTreeNodeNonLeaf split = (BTreeNodeNonLeaf) bTreeNonLeaf.split(5);

        final StringBuilder printer = new StringBuilder();
        bTreeNonLeaf.print(printer, "root");
        System.out.println("printing after split");
        System.out.println(printer.toString());
        System.out.println("end printing");

        final StringBuilder printer3 = new StringBuilder();
        split.print(printer3, "root");
        System.out.println("printing split");
        System.out.println(printer3.toString());
        System.out.println("end printing");

        assertEquals(5, bTreeNonLeaf.getKeyCount());
        assertEquals(5, split.getKeyCount());
    }

    private BTreeNodeLeaf createLeafNodeWithKeys(final int numKeys, final int startKey)
    {
        final BTreeNodeLeaf bTreeNode = new BTreeNodeLeaf();
        for (int i = 0; i < numKeys; i++)
        {
            final int keyNum = startKey + i;
            final ByteBuffer key = createValue("key" + keyNum);
            final ByteBuffer value = createValue("value" + keyNum);

            bTreeNode.insert(key, value);
        }

        return bTreeNode;
    }
}