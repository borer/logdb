package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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

        bTreeNonLeaf.insertChild(0, key, bTreeNode);
    }

    @Test
    void shouldBeAbleToInsertMultipleChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }
    }

    @Test
    void shouldBeAbleToDeleteChildren()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);
        }

        assertEquals(10, bTreeNonLeaf.getKeyCount());

        final ByteBuffer key = createValue("key" + (numKeysPerChild * 9));
        bTreeNonLeaf.remove(key);
        assertEquals(9, bTreeNonLeaf.getKeyCount());

        final ByteBuffer key2 = createValue("key" + (numKeysPerChild * 8));
        bTreeNonLeaf.remove(key2);
        assertEquals(8, bTreeNonLeaf.getKeyCount());
    }

    @Test
    void shouldPrintNode()
    {
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
                "root[label = \"<key0> |key0|<key5> |key5|<key10> |key10|<key15> |key15|<key20> |key20|<key25> |key25|<key30> |key30|<key35> |key35|<key40> |key40|<key45> |key45| <lastKeyroot>\"];\n" +
                "\"root\":key0 -> \"key0\"\n" +
                "\"root\":key5 -> \"key5\"\n" +
                "\"root\":key10 -> \"key10\"\n" +
                "\"root\":key15 -> \"key15\"\n" +
                "\"root\":key20 -> \"key20\"\n" +
                "\"root\":key25 -> \"key25\"\n" +
                "\"root\":key30 -> \"key30\"\n" +
                "\"root\":key35 -> \"key35\"\n" +
                "\"root\":key40 -> \"key40\"\n" +
                "\"root\":key45 -> \"key45\"\n" +
                "\"root\":lastKeyroot -> \"lastKeyroot\"\n" +
                "key0[label = \"<key0> |value0|<key1> |value1|<key2> |value2|<key3> |value3|<key4> |value4|\"];\n" +
                "key5[label = \"<key5> |value5|<key6> |value6|<key7> |value7|<key8> |value8|<key9> |value9|\"];\n" +
                "key10[label = \"<key10> |value10|<key11> |value11|<key12> |value12|<key13> |value13|<key14> |value14|\"];\n" +
                "key15[label = \"<key15> |value15|<key16> |value16|<key17> |value17|<key18> |value18|<key19> |value19|\"];\n" +
                "key20[label = \"<key20> |value20|<key21> |value21|<key22> |value22|<key23> |value23|<key24> |value24|\"];\n" +
                "key25[label = \"<key25> |value25|<key26> |value26|<key27> |value27|<key28> |value28|<key29> |value29|\"];\n" +
                "key30[label = \"<key30> |value30|<key31> |value31|<key32> |value32|<key33> |value33|<key34> |value34|\"];\n" +
                "key35[label = \"<key35> |value35|<key36> |value36|<key37> |value37|<key38> |value38|<key39> |value39|\"];\n" +
                "key40[label = \"<key40> |value40|<key41> |value41|<key42> |value42|<key43> |value43|<key44> |value44|\"];\n" +
                "key45[label = \"<key45> |value45|<key46> |value46|<key47> |value47|<key48> |value48|<key49> |value49|\"];\n" +
                "lastKeyroot[label = \"<key50> |value50|<key51> |value51|<key52> |value52|<key53> |value53|<key54> |value54|\"];\n" +
                "}\n";

        final StringBuilder printer = new StringBuilder();
        bTreeNonLeaf.print(printer, "root");

        assertEquals(expectedDotString, printer.toString());
    }

    @Test
    void shouldBeAbleToSplitInTwoNodes()
    {
        final int numKeysPerChild = 5;
        final int expectedKeysInCurrent = 5;
        final int expectedKeysInSplit = 4;
        for (int i = 0; i < 10; i++)
        {
            final String keyValue = "key" + (numKeysPerChild * i);
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final BTreeNode child = new BTreeNodeLeaf();

            bTreeNonLeaf.insertChild(i, childKeyBuffer, child);
        }

        //first 5 children have first 25 keys
        final ByteBuffer keyUsedForSplit = bTreeNonLeaf.getKeyAtIndex(5);

        final BTreeNodeNonLeaf split = (BTreeNodeNonLeaf) bTreeNonLeaf.split(5);

        assertEquals(expectedKeysInCurrent, bTreeNonLeaf.getKeyCount());
        assertEquals(expectedKeysInSplit, split.getKeyCount());

        for (int i = 0; i < expectedKeysInCurrent; i++)
        {
            final ByteBuffer keyAtIndex = bTreeNonLeaf.getKeyAtIndex(i);
            assertNotEquals(keyUsedForSplit, keyAtIndex);
        }

        for (int i = 0; i < expectedKeysInSplit; i++)
        {
            final ByteBuffer keyAtIndex = split.getKeyAtIndex(i);
            assertNotEquals(keyUsedForSplit, keyAtIndex);
        }
    }

    @Test
    void shouldDeepCopyNonLeafNode()
    {
        final int numKeysPerChild = 5;
        for (int i = 0; i < 10; i++)
        {
            final BTreeNode bTreeNode = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * i));
            final ByteBuffer key = createValue("key" + (numKeysPerChild * i));

            bTreeNonLeaf.insertChild(i, key, bTreeNode);

            assertEquals(i + 1, bTreeNonLeaf.getKeyCount());
        }

        BTreeNode copy = bTreeNonLeaf.copy();

        final BTreeNode child = createLeafNodeWithKeys(numKeysPerChild, (numKeysPerChild * 10));
        final ByteBuffer key = createValue("key" + (numKeysPerChild * 10));
        bTreeNonLeaf.insertChild(10, key, child);

        assertEquals(11, bTreeNonLeaf.getKeyCount());
        assertEquals(10, copy.getKeyCount());
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