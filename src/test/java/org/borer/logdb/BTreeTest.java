package org.borer.logdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.borer.logdb.TestUtils.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BTreeTest
{
    private BTree bTree;

    @BeforeEach
    void setUp()
    {
        bTree = new BTree(new BTreeNodeLeaf());
    }

    @Test
    void shouldBeAbleToInsert100Elements()
    {
        for (int i = 0; i < 100; i++)
        {
            final String keyValue = "key" + i;
            final ByteBuffer childKeyBuffer = createValue(keyValue);
            final ByteBuffer childValueBuffer = createValue(keyValue);

            bTree.insert(childKeyBuffer, childValueBuffer);
        }

        StringBuilder printer= new StringBuilder();
        bTree.print(printer);

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "root[label = \"<key4> |key4|<key67> |key67| <lastKeyroot>\"];\n" +
                "\"root\":key4 -> \"key4\"\n" +
                "\"root\":key67 -> \"key67\"\n" +
                "\"root\":lastKeyroot -> \"lastKeyroot\"\n" +
                "key4[label = \"<key13> |key13|<key18> |key18|<key22> |key22|<key27> |key27|<key31> |key31| <lastKeykey4>\"];\n" +
                "\"key4\":key13 -> \"key13\"\n" +
                "\"key4\":key18 -> \"key18\"\n" +
                "\"key4\":key22 -> \"key22\"\n" +
                "\"key4\":key27 -> \"key27\"\n" +
                "\"key4\":key31 -> \"key31\"\n" +
                "\"key4\":lastKeykey4 -> \"lastKeykey4\"\n" +
                "key13[label = \"<key0> |key0|<key1> |key1|<key10> |key10|<key11> |key11|<key12> |key12|\"];\n" +
                "key18[label = \"<key13> |key13|<key14> |key14|<key15> |key15|<key16> |key16|<key17> |key17|\"];\n" +
                "key22[label = \"<key18> |key18|<key19> |key19|<key2> |key2|<key20> |key20|<key21> |key21|\"];\n" +
                "key27[label = \"<key22> |key22|<key23> |key23|<key24> |key24|<key25> |key25|<key26> |key26|\"];\n" +
                "key31[label = \"<key27> |key27|<key28> |key28|<key29> |key29|<key3> |key3|<key30> |key30|\"];\n" +
                "lastKeykey4[label = \"<key31> |key31|<key32> |key32|<key33> |key33|<key34> |key34|<key35> |key35|<key36> |key36|<key37> |key37|<key38> |key38|<key39> |key39|\"];\n" +
                "key67[label = \"<key44> |key44|<key49> |key49|<key53> |key53|<key58> |key58|<key62> |key62| <lastKeykey67>\"];\n" +
                "\"key67\":key44 -> \"key44\"\n" +
                "\"key67\":key49 -> \"key49\"\n" +
                "\"key67\":key53 -> \"key53\"\n" +
                "\"key67\":key58 -> \"key58\"\n" +
                "\"key67\":key62 -> \"key62\"\n" +
                "\"key67\":lastKeykey67 -> \"lastKeykey67\"\n" +
                "key44[label = \"<key4> |key4|<key40> |key40|<key41> |key41|<key42> |key42|<key43> |key43|\"];\n" +
                "key49[label = \"<key44> |key44|<key45> |key45|<key46> |key46|<key47> |key47|<key48> |key48|\"];\n" +
                "key53[label = \"<key49> |key49|<key5> |key5|<key50> |key50|<key51> |key51|<key52> |key52|\"];\n" +
                "key58[label = \"<key53> |key53|<key54> |key54|<key55> |key55|<key56> |key56|<key57> |key57|\"];\n" +
                "key62[label = \"<key58> |key58|<key59> |key59|<key6> |key6|<key60> |key60|<key61> |key61|\"];\n" +
                "lastKeykey67[label = \"<key62> |key62|<key63> |key63|<key64> |key64|<key65> |key65|<key66> |key66|\"];\n" +
                "lastKeyroot[label = \"<key71> |key71|<key76> |key76|<key80> |key80|<key85> |key85|<key9> |key9|<key94> |key94| <lastKeylastKeyroot>\"];\n" +
                "\"lastKeyroot\":key71 -> \"key71\"\n" +
                "\"lastKeyroot\":key76 -> \"key76\"\n" +
                "\"lastKeyroot\":key80 -> \"key80\"\n" +
                "\"lastKeyroot\":key85 -> \"key85\"\n" +
                "\"lastKeyroot\":key9 -> \"key9\"\n" +
                "\"lastKeyroot\":key94 -> \"key94\"\n" +
                "\"lastKeyroot\":lastKeylastKeyroot -> \"lastKeylastKeyroot\"\n" +
                "key71[label = \"<key67> |key67|<key68> |key68|<key69> |key69|<key7> |key7|<key70> |key70|\"];\n" +
                "key76[label = \"<key71> |key71|<key72> |key72|<key73> |key73|<key74> |key74|<key75> |key75|\"];\n" +
                "key80[label = \"<key76> |key76|<key77> |key77|<key78> |key78|<key79> |key79|<key8> |key8|\"];\n" +
                "key85[label = \"<key80> |key80|<key81> |key81|<key82> |key82|<key83> |key83|<key84> |key84|\"];\n" +
                "key9[label = \"<key85> |key85|<key86> |key86|<key87> |key87|<key88> |key88|<key89> |key89|\"];\n" +
                "key94[label = \"<key9> |key9|<key90> |key90|<key91> |key91|<key92> |key92|<key93> |key93|\"];\n" +
                "lastKeylastKeyroot[label = \"<key94> |key94|<key95> |key95|<key96> |key96|<key97> |key97|<key98> |key98|<key99> |key99|\"];\n" +
                "}\n";

        assertEquals(expectedTree, printer.toString());
    }
}