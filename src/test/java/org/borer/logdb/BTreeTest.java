package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

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
                "\"88\"[label = \" <key4> |key4|  <key67> |key67|  <lastChild> Ls| \"];\n" +
                "\"88\":key4 -> \"76\"\n" +
                "\"88\":key67 -> \"87\"\n" +
                "\"88\":lastChild -> \"95\"\n" +
                "\"76\"[label = \" <key13> |key13|  <key18> |key18|  <key22> |key22|  <key27> |key27|  <key31> |key31|  <lastChild> Ls|  <rightSibling> R| \"];\n" +
                "\"76\":key13 -> \"74\"\n" +
                "\"76\":key18 -> \"77\"\n" +
                "\"76\":key22 -> \"78\"\n" +
                "\"76\":key27 -> \"79\"\n" +
                "\"76\":key31 -> \"80\"\n" +
                "\"76\":lastChild -> \"81\"\n" +
                "\"76\":rightSibling -> \"87\" [style=dashed, color=grey]\n" +
                "\"74\"[label = \" <key0> |key0|  <key1> |key1|  <key10> |key10|  <key11> |key11|  <key12> |key12|  <rightSibling> R \"];\n" +
                "\"74\":rightSibling -> \"77\" [style=dashed, color=grey]\n" +
                "\"77\"[label = \" <leftSibling> L|  <key13> |key13|  <key14> |key14|  <key15> |key15|  <key16> |key16|  <key17> |key17|  <rightSibling> R \"];\n" +
                "\"77\":leftSibling -> \"74\" [style=dashed, color=grey]\n" +
                "\"77\":rightSibling -> \"78\" [style=dashed, color=grey]\n" +
                "\"78\"[label = \" <leftSibling> L|  <key18> |key18|  <key19> |key19|  <key2> |key2|  <key20> |key20|  <key21> |key21|  <rightSibling> R \"];\n" +
                "\"78\":leftSibling -> \"77\" [style=dashed, color=grey]\n" +
                "\"78\":rightSibling -> \"79\" [style=dashed, color=grey]\n" +
                "\"79\"[label = \" <leftSibling> L|  <key22> |key22|  <key23> |key23|  <key24> |key24|  <key25> |key25|  <key26> |key26|  <rightSibling> R \"];\n" +
                "\"79\":leftSibling -> \"78\" [style=dashed, color=grey]\n" +
                "\"79\":rightSibling -> \"80\" [style=dashed, color=grey]\n" +
                "\"80\"[label = \" <leftSibling> L|  <key27> |key27|  <key28> |key28|  <key29> |key29|  <key3> |key3|  <key30> |key30|  <rightSibling> R \"];\n" +
                "\"80\":leftSibling -> \"79\" [style=dashed, color=grey]\n" +
                "\"80\":rightSibling -> \"81\" [style=dashed, color=grey]\n" +
                "\"81\"[label = \" <leftSibling> L|  <key31> |key31|  <key32> |key32|  <key33> |key33|  <key34> |key34|  <key35> |key35|  <key36> |key36|  <key37> |key37|  <key38> |key38|  <key39> |key39|  <rightSibling> R \"];\n" +
                "\"81\":leftSibling -> \"80\" [style=dashed, color=grey]\n" +
                "\"81\":rightSibling -> \"75\" [style=dashed, color=grey]\n" +
                "\"87\"[label = \" <leftSibling> L|  <key44> |key44|  <key49> |key49|  <key53> |key53|  <key58> |key58|  <key62> |key62|  <lastChild> Ls|  <rightSibling> R| \"];\n" +
                "\"87\":key44 -> \"75\"\n" +
                "\"87\":key49 -> \"82\"\n" +
                "\"87\":key53 -> \"83\"\n" +
                "\"87\":key58 -> \"84\"\n" +
                "\"87\":key62 -> \"85\"\n" +
                "\"87\":lastChild -> \"86\"\n" +
                "\"87\":leftSibling -> \"76\" [style=dashed, color=grey]\n" +
                "\"87\":rightSibling -> \"95\" [style=dashed, color=grey]\n" +
                "\"75\"[label = \" <leftSibling> L|  <key4> |key4|  <key40> |key40|  <key41> |key41|  <key42> |key42|  <key43> |key43|  <rightSibling> R \"];\n" +
                "\"75\":leftSibling -> \"81\" [style=dashed, color=grey]\n" +
                "\"75\":rightSibling -> \"82\" [style=dashed, color=grey]\n" +
                "\"82\"[label = \" <leftSibling> L|  <key44> |key44|  <key45> |key45|  <key46> |key46|  <key47> |key47|  <key48> |key48|  <rightSibling> R \"];\n" +
                "\"82\":leftSibling -> \"75\" [style=dashed, color=grey]\n" +
                "\"82\":rightSibling -> \"83\" [style=dashed, color=grey]\n" +
                "\"83\"[label = \" <leftSibling> L|  <key49> |key49|  <key5> |key5|  <key50> |key50|  <key51> |key51|  <key52> |key52|  <rightSibling> R \"];\n" +
                "\"83\":leftSibling -> \"82\" [style=dashed, color=grey]\n" +
                "\"83\":rightSibling -> \"84\" [style=dashed, color=grey]\n" +
                "\"84\"[label = \" <leftSibling> L|  <key53> |key53|  <key54> |key54|  <key55> |key55|  <key56> |key56|  <key57> |key57|  <rightSibling> R \"];\n" +
                "\"84\":leftSibling -> \"83\" [style=dashed, color=grey]\n" +
                "\"84\":rightSibling -> \"85\" [style=dashed, color=grey]\n" +
                "\"85\"[label = \" <leftSibling> L|  <key58> |key58|  <key59> |key59|  <key6> |key6|  <key60> |key60|  <key61> |key61|  <rightSibling> R \"];\n" +
                "\"85\":leftSibling -> \"84\" [style=dashed, color=grey]\n" +
                "\"85\":rightSibling -> \"86\" [style=dashed, color=grey]\n" +
                "\"86\"[label = \" <leftSibling> L|  <key62> |key62|  <key63> |key63|  <key64> |key64|  <key65> |key65|  <key66> |key66|  <rightSibling> R \"];\n" +
                "\"86\":leftSibling -> \"85\" [style=dashed, color=grey]\n" +
                "\"86\":rightSibling -> \"89\" [style=dashed, color=grey]\n" +
                "\"95\"[label = \" <leftSibling> L|  <key71> |key71|  <key76> |key76|  <key80> |key80|  <key85> |key85|  <key9> |key9|  <key94> |key94|  <lastChild> Ls| \"];\n" +
                "\"95\":key71 -> \"89\"\n" +
                "\"95\":key76 -> \"90\"\n" +
                "\"95\":key80 -> \"91\"\n" +
                "\"95\":key85 -> \"92\"\n" +
                "\"95\":key9 -> \"93\"\n" +
                "\"95\":key94 -> \"94\"\n" +
                "\"95\":lastChild -> \"96\"\n" +
                "\"95\":leftSibling -> \"87\" [style=dashed, color=grey]\n" +
                "\"89\"[label = \" <leftSibling> L|  <key67> |key67|  <key68> |key68|  <key69> |key69|  <key7> |key7|  <key70> |key70|  <rightSibling> R \"];\n" +
                "\"89\":leftSibling -> \"86\" [style=dashed, color=grey]\n" +
                "\"89\":rightSibling -> \"90\" [style=dashed, color=grey]\n" +
                "\"90\"[label = \" <leftSibling> L|  <key71> |key71|  <key72> |key72|  <key73> |key73|  <key74> |key74|  <key75> |key75|  <rightSibling> R \"];\n" +
                "\"90\":leftSibling -> \"89\" [style=dashed, color=grey]\n" +
                "\"90\":rightSibling -> \"91\" [style=dashed, color=grey]\n" +
                "\"91\"[label = \" <leftSibling> L|  <key76> |key76|  <key77> |key77|  <key78> |key78|  <key79> |key79|  <key8> |key8|  <rightSibling> R \"];\n" +
                "\"91\":leftSibling -> \"90\" [style=dashed, color=grey]\n" +
                "\"91\":rightSibling -> \"92\" [style=dashed, color=grey]\n" +
                "\"92\"[label = \" <leftSibling> L|  <key80> |key80|  <key81> |key81|  <key82> |key82|  <key83> |key83|  <key84> |key84|  <rightSibling> R \"];\n" +
                "\"92\":leftSibling -> \"91\" [style=dashed, color=grey]\n" +
                "\"92\":rightSibling -> \"93\" [style=dashed, color=grey]\n" +
                "\"93\"[label = \" <leftSibling> L|  <key85> |key85|  <key86> |key86|  <key87> |key87|  <key88> |key88|  <key89> |key89|  <rightSibling> R \"];\n" +
                "\"93\":leftSibling -> \"92\" [style=dashed, color=grey]\n" +
                "\"93\":rightSibling -> \"94\" [style=dashed, color=grey]\n" +
                "\"94\"[label = \" <leftSibling> L|  <key9> |key9|  <key90> |key90|  <key91> |key91|  <key92> |key92|  <key93> |key93|  <rightSibling> R \"];\n" +
                "\"94\":leftSibling -> \"93\" [style=dashed, color=grey]\n" +
                "\"94\":rightSibling -> \"96\" [style=dashed, color=grey]\n" +
                "\"96\"[label = \" <leftSibling> L|  <key94> |key94|  <key95> |key95|  <key96> |key96|  <key97> |key97|  <key98> |key98|  <key99> |key99| \"];\n" +
                "\"96\":leftSibling -> \"94\" [style=dashed, color=grey]\n" +
                "}\n";

        assertEquals(expectedTree, printer.toString());
    }
}