package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BTreeTest
{
    private BTree bTree;

    @BeforeEach
    void setUp()
    {
        bTree = new BTree(new BTreeNodeLeaf());
    }

    @Test
    void shouldBeAbleToGetElementsFromPast()
    {
        final String keyValue = "key5";
        final ByteBuffer childKeyBuffer = createValue(keyValue);
        final String expectedValue1 = keyValue + "1";
        final String expectedValue2 = keyValue + "2";
        final String expectedValue3 = keyValue + "3";
        final ByteBuffer childValueBuffer1 = createValue(expectedValue1);
        final ByteBuffer childValueBuffer2 = createValue(expectedValue2);
        final ByteBuffer childValueBuffer3 = createValue(expectedValue3);

        bTree.put(childKeyBuffer, childValueBuffer1);
        bTree.put(childKeyBuffer, childValueBuffer2);
        bTree.put(childKeyBuffer, childValueBuffer3);

        final ByteBuffer actual0 = bTree.get(childKeyBuffer, 0);
        assertNull(actual0);

        //index is 0 based, but the version 0 is from the constructor
        final ByteBuffer actual1 = bTree.get(childKeyBuffer, 1);
        assertEquals(expectedValue1, new String(actual1.array()));

        final ByteBuffer actual2 = bTree.get(childKeyBuffer, 2);
        assertEquals(expectedValue2, new String(actual2.array()));

        final ByteBuffer actual3 = bTree.get(childKeyBuffer, 3);
        assertEquals(expectedValue3, new String(actual3.array()));

        //get latest version by default
        final ByteBuffer actualLatest = bTree.get(childKeyBuffer);
        assertEquals(expectedValue3, new String(actualLatest.array()));
    }

    @Test
    void shouldConsumeKeyValuesInOrder()
    {
        final List<String> expectedOrder = new ArrayList<>();
        for (int i = 0; i < 100; i++)
        {
            final String keyValue = String.valueOf(i);
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final ByteBuffer childValueBuffer = createValue(keyValue);

            expectedOrder.add(keyValue);

            bTree.put(childKeyBuffer, childValueBuffer);
        }

        expectedOrder.sort((o1, o2) ->
        {
            final ByteBuffer value1 = createValue(o1);
            final ByteBuffer value2 = createValue(o2);
            return value1.compareTo(value2);
        });

        final LinkedList<String> actualOrder = new LinkedList<>();
        bTree.consumeAll((key, value) -> actualOrder.addLast(new String(key.array())));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }
    }

    @Test
    void shouldConsumeKeyValuesInOrderForAGivenVersion()
    {
        final List<String> expectedOrder = new ArrayList<>();
        final int version = 50;
        for (int i = 0; i < 100; i++)
        {
            final String keyValue = String.valueOf(i);
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final ByteBuffer childValueBuffer = createValue(keyValue);

            if (i < version)
            {
                expectedOrder.add(keyValue);
            }

            bTree.put(childKeyBuffer, childValueBuffer);
        }

        expectedOrder.sort((o1, o2) ->
        {
            final ByteBuffer value1 = createValue(o1);
            final ByteBuffer value2 = createValue(o2);
            return value1.compareTo(value2);
        });

        final LinkedList<String> actualOrder = new LinkedList<>();
        bTree.consumeAll(version, (key, value) -> actualOrder.addLast(new String(key.array())));

        assertEquals(expectedOrder.size(), actualOrder.size());

        for (int i = 0; i < expectedOrder.size(); i++)
        {
            assertEquals(expectedOrder.get(i), actualOrder.get(i));
        }
    }

    @Test
    void shouldBeAbleToInsert100Elements()
    {
        for (int i = 0; i < 100; i++)
        {
            final String keyValue = "key" + i;
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final ByteBuffer childValueBuffer = createValue(keyValue);

            bTree.put(childKeyBuffer, childValueBuffer);
        }

        StringBuilder printer= new StringBuilder();
        bTree.print(printer);

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"134\"[label = \" <key4> |key4|  <key67> |key67|  <lastChild> |Ls \"];\n" +
                "\"134\":key4 -> \"122\"\n" +
                "\"134\":key67 -> \"133\"\n" +
                "\"134\":lastChild -> \"141\"\n" +
                "\"122\"[label = \" <key13> |key13|  <key18> |key18|  <key22> |key22|  <key27> |key27|  <key31> |key31|  <lastChild> |Ls \"];\n" +
                "\"122\":key13 -> \"120\"\n" +
                "\"122\":key18 -> \"123\"\n" +
                "\"122\":key22 -> \"124\"\n" +
                "\"122\":key27 -> \"125\"\n" +
                "\"122\":key31 -> \"126\"\n" +
                "\"122\":lastChild -> \"127\"\n" +
                "\"120\"[label = \" <key0> |key0|  <key1> |key1|  <key10> |key10|  <key11> |key11|  <key12> |key12| \"];\n" +
                "\"123\"[label = \" <key13> |key13|  <key14> |key14|  <key15> |key15|  <key16> |key16|  <key17> |key17| \"];\n" +
                "\"124\"[label = \" <key18> |key18|  <key19> |key19|  <key2> |key2|  <key20> |key20|  <key21> |key21| \"];\n" +
                "\"125\"[label = \" <key22> |key22|  <key23> |key23|  <key24> |key24|  <key25> |key25|  <key26> |key26| \"];\n" +
                "\"126\"[label = \" <key27> |key27|  <key28> |key28|  <key29> |key29|  <key3> |key3|  <key30> |key30| \"];\n" +
                "\"127\"[label = \" <key31> |key31|  <key32> |key32|  <key33> |key33|  <key34> |key34|  <key35> |key35|  <key36> |key36|  <key37> |key37|  <key38> |key38|  <key39> |key39| \"];\n" +
                "\"133\"[label = \" <key44> |key44|  <key49> |key49|  <key53> |key53|  <key58> |key58|  <key62> |key62|  <lastChild> |Ls \"];\n" +
                "\"133\":key44 -> \"121\"\n" +
                "\"133\":key49 -> \"128\"\n" +
                "\"133\":key53 -> \"129\"\n" +
                "\"133\":key58 -> \"130\"\n" +
                "\"133\":key62 -> \"131\"\n" +
                "\"133\":lastChild -> \"132\"\n" +
                "\"121\"[label = \" <key4> |key4|  <key40> |key40|  <key41> |key41|  <key42> |key42|  <key43> |key43| \"];\n" +
                "\"128\"[label = \" <key44> |key44|  <key45> |key45|  <key46> |key46|  <key47> |key47|  <key48> |key48| \"];\n" +
                "\"129\"[label = \" <key49> |key49|  <key5> |key5|  <key50> |key50|  <key51> |key51|  <key52> |key52| \"];\n" +
                "\"130\"[label = \" <key53> |key53|  <key54> |key54|  <key55> |key55|  <key56> |key56|  <key57> |key57| \"];\n" +
                "\"131\"[label = \" <key58> |key58|  <key59> |key59|  <key6> |key6|  <key60> |key60|  <key61> |key61| \"];\n" +
                "\"132\"[label = \" <key62> |key62|  <key63> |key63|  <key64> |key64|  <key65> |key65|  <key66> |key66| \"];\n" +
                "\"141\"[label = \" <key71> |key71|  <key76> |key76|  <key80> |key80|  <key85> |key85|  <key9> |key9|  <key94> |key94|  <lastChild> |Ls \"];\n" +
                "\"141\":key71 -> \"135\"\n" +
                "\"141\":key76 -> \"136\"\n" +
                "\"141\":key80 -> \"137\"\n" +
                "\"141\":key85 -> \"138\"\n" +
                "\"141\":key9 -> \"139\"\n" +
                "\"141\":key94 -> \"140\"\n" +
                "\"141\":lastChild -> \"142\"\n" +
                "\"135\"[label = \" <key67> |key67|  <key68> |key68|  <key69> |key69|  <key7> |key7|  <key70> |key70| \"];\n" +
                "\"136\"[label = \" <key71> |key71|  <key72> |key72|  <key73> |key73|  <key74> |key74|  <key75> |key75| \"];\n" +
                "\"137\"[label = \" <key76> |key76|  <key77> |key77|  <key78> |key78|  <key79> |key79|  <key8> |key8| \"];\n" +
                "\"138\"[label = \" <key80> |key80|  <key81> |key81|  <key82> |key82|  <key83> |key83|  <key84> |key84| \"];\n" +
                "\"139\"[label = \" <key85> |key85|  <key86> |key86|  <key87> |key87|  <key88> |key88|  <key89> |key89| \"];\n" +
                "\"140\"[label = \" <key9> |key9|  <key90> |key90|  <key91> |key91|  <key92> |key92|  <key93> |key93| \"];\n" +
                "\"142\"[label = \" <key94> |key94|  <key95> |key95|  <key96> |key96|  <key97> |key97|  <key98> |key98|  <key99> |key99| \"];\n" +
                "}\n";

        assertEquals(expectedTree, printer.toString());
    }
}