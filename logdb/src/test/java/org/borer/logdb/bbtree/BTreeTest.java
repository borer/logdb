package org.borer.logdb.bbtree;

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
import static org.junit.jupiter.api.Assertions.fail;

class BTreeTest
{
    private BTree bTree;
    private NodesManager nodesManager;

    @BeforeEach
    void setUp()
    {
        final Storage storage = new MemoryStorage(TestUtils.BYTE_ORDER, 512);

        nodesManager = new NodesManager(storage);
        bTree = new BTree(nodesManager);
    }

    @Test
    void shouldBeAbleToPutElementsWithLog()
    {
        for (long i = 0; i < 200; i++)
        {
            bTree.putWithLog(i, i);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"250\"[label = \"{ log | { 183-183 |  184-184 |  185-185 |  186-186 |  187-187 |  188-188 |  189-189 |  190-190 |  191-191 |  192-192 |  193-193 |  194-194 |  195-195 |  196-196 |  197-197 |  198-198 |  199-199 }}| <91> |40|  <159> |80|  <192> |120|  <lastChild> |Ls \"];\n" +
                "\"250\":91 -> \"91\"\n" +
                "\"250\":159 -> \"159\"\n" +
                "\"250\":192 -> \"192\"\n" +
                "\"250\":lastChild -> \"228\"\n" +
                "\"91\"[label = \" <11> |5|  <41> |10|  <42> |15|  <43> |20|  <44> |25|  <45> |30|  <69> |35|  <lastChild> |Ls \"];\n" +
                "\"91\":11 -> \"11\"\n" +
                "\"91\":41 -> \"41\"\n" +
                "\"91\":42 -> \"42\"\n" +
                "\"91\":43 -> \"43\"\n" +
                "\"91\":44 -> \"44\"\n" +
                "\"91\":45 -> \"45\"\n" +
                "\"91\":69 -> \"69\"\n" +
                "\"91\":lastChild -> \"70\"\n" +
                "\"11\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"41\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"42\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"43\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"44\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"45\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"69\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"70\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"159\"[label = \" <71> |45|  <72> |50|  <92> |55|  <93> |60|  <94> |65|  <95> |70|  <127> |75|  <lastChild> |Ls \"];\n" +
                "\"159\":71 -> \"71\"\n" +
                "\"159\":72 -> \"72\"\n" +
                "\"159\":92 -> \"92\"\n" +
                "\"159\":93 -> \"93\"\n" +
                "\"159\":94 -> \"94\"\n" +
                "\"159\":95 -> \"95\"\n" +
                "\"159\":127 -> \"127\"\n" +
                "\"159\":lastChild -> \"128\"\n" +
                "\"71\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"72\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"92\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "\"93\"[label = \" <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"94\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"95\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"127\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"128\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                "\"192\"[label = \" <129> |85|  <130> |90|  <160> |95|  <161> |100|  <162> |105|  <163> |110|  <193> |115|  <lastChild> |Ls \"];\n" +
                "\"192\":129 -> \"129\"\n" +
                "\"192\":130 -> \"130\"\n" +
                "\"192\":160 -> \"160\"\n" +
                "\"192\":161 -> \"161\"\n" +
                "\"192\":162 -> \"162\"\n" +
                "\"192\":163 -> \"163\"\n" +
                "\"192\":193 -> \"193\"\n" +
                "\"192\":lastChild -> \"194\"\n" +
                "\"129\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84| \"];\n" +
                "\"130\"[label = \" <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"160\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94| \"];\n" +
                "\"161\"[label = \" <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "\"162\"[label = \" <100> |100|  <101> |101|  <102> |102|  <103> |103|  <104> |104| \"];\n" +
                "\"163\"[label = \" <105> |105|  <106> |106|  <107> |107|  <108> |108|  <109> |109| \"];\n" +
                "\"193\"[label = \" <110> |110|  <111> |111|  <112> |112|  <113> |113|  <114> |114| \"];\n" +
                "\"194\"[label = \" <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"228\"[label = \"{ log | { 179-179 |  180-180 |  181-181 }}| <195> |125|  <196> |130|  <197> |135|  <198> |140|  <199> |145|  <200> |150|  <229> |155|  <230> |160|  <231> |165|  <232> |170|  <lastChild> |Ls \"];\n" +
                "\"228\":195 -> \"195\"\n" +
                "\"228\":196 -> \"196\"\n" +
                "\"228\":197 -> \"197\"\n" +
                "\"228\":198 -> \"198\"\n" +
                "\"228\":199 -> \"199\"\n" +
                "\"228\":200 -> \"200\"\n" +
                "\"228\":229 -> \"229\"\n" +
                "\"228\":230 -> \"230\"\n" +
                "\"228\":231 -> \"231\"\n" +
                "\"228\":232 -> \"232\"\n" +
                "\"228\":lastChild -> \"233\"\n" +
                "\"195\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124| \"];\n" +
                "\"196\"[label = \" <125> |125|  <126> |126|  <127> |127|  <128> |128|  <129> |129| \"];\n" +
                "\"197\"[label = \" <130> |130|  <131> |131|  <132> |132|  <133> |133|  <134> |134| \"];\n" +
                "\"198\"[label = \" <135> |135|  <136> |136|  <137> |137|  <138> |138|  <139> |139| \"];\n" +
                "\"199\"[label = \" <140> |140|  <141> |141|  <142> |142|  <143> |143|  <144> |144| \"];\n" +
                "\"200\"[label = \" <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"229\"[label = \" <150> |150|  <151> |151|  <152> |152|  <153> |153|  <154> |154| \"];\n" +
                "\"230\"[label = \" <155> |155|  <156> |156|  <157> |157|  <158> |158|  <159> |159| \"];\n" +
                "\"231\"[label = \" <160> |160|  <161> |161|  <162> |162|  <163> |163|  <164> |164| \"];\n" +
                "\"232\"[label = \" <165> |165|  <166> |166|  <167> |167|  <168> |168|  <169> |169| \"];\n" +
                "\"233\"[label = \" <170> |170|  <171> |171|  <172> |172|  <173> |173|  <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <182> |182| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }

    @Test
    void shouldBeAbleToRetrieveElementsWithLog()
    {
        for (long i = 0; i < 200; i++)
        {
            bTree.putWithLog(i, i);
        }

        for (long i = 0; i < 200; i++)
        {
            assertEquals(i, bTree.getWithLog(i));
        }
    }

    @Test
    void shouldBeAbleToRetrieveNonExistingElementsWithLog()
    {
        for (long i = 0; i < 10; i++)
        {
            assertEquals(-1, bTree.getWithLog(i));
        }
    }

    @Test
    void shouldBeAbleToRetrievePastVersionsForElementsWithLog()
    {
        final long key = 123L;
        for (long i = 0; i < 200; i++)
        {
            bTree.putWithLog(key, i);
        }

        for (long i = 0; i < 200; i++)
        {
            assertEquals(i, bTree.getWithLog(key, (int)i));
        }
    }

    @Test
    void shouldNotFailToDeleteNonExistingKeyWithLogWithoutFalsePositives()
    {
        assertEquals(1, bTree.getNodesCount());

        final int numberOfPairs = 200;
        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithLogWithoutFalsePositives(i);
        }

        for (long i = 0; i < 200; i++)
        {
            bTree.putWithLog(i, i);
        }

        assertEquals(38, bTree.getNodesCount());

        bTree.removeWithLogWithoutFalsePositives(200);
        bTree.removeWithLogWithoutFalsePositives(-20);

        assertEquals(38, bTree.getNodesCount());
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWithoutFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.putWithLog(i, i);
        }

        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithLogWithoutFalsePositives(i);
        }

        assertEquals(1L, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"765\"[label = \" <571> |120|  <lastChild> |Ls \"];\n" +
                "\"765\":571 -> \"571\"\n" +
                "\"765\":lastChild -> \"747\"\n" +
                "\"571\"[label = \"\"];\n" +
                "\"747\"[label = \"\"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }

    @Test
    void shouldNotFailToDeleteNonExistingKeyWithLogWithFalsePositives()
    {
        assertEquals(1, bTree.getNodesCount());

        final int numberOfPairs = 200;
        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithLog(i);
        }

        for (long i = 0; i < 200; i++)
        {
            bTree.putWithLog(i, i);
        }

        assertEquals(38, bTree.getNodesCount());

        bTree.removeWithLog(200);
        bTree.removeWithLog(-20);

        assertEquals(38, bTree.getNodesCount());
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWitFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.putWithLog(i, i);
        }

        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithLog(i);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"488\"[label = \" <322> |40|  <347> |80|  <408> |120|  <464> |160|  <lastChild> |Ls \"];\n" +
                "\"488\":322 -> \"322\"\n" +
                "\"488\":347 -> \"347\"\n" +
                "\"488\":408 -> \"408\"\n" +
                "\"488\":464 -> \"464\"\n" +
                "\"488\":lastChild -> \"489\"\n" +
                "\"322\"[label = \"{ log | { 20--1 |  21--1 |  22--1 |  23--1 |  24--1 |  25--1 |  26--1 |  27--1 |  28--1 |  29--1 |  30--1 |  32--1 |  33--1 |  34--1 |  35--1 |  36--1 |  37--1 |  38--1 |  39--1 }}| <293> |5|  <294> |10|  <295> |15|  <44> |25|  <45> |30|  <296> |35|  <lastChild> |Ls \"];\n" +
                "\"322\":293 -> \"293\"\n" +
                "\"322\":294 -> \"294\"\n" +
                "\"322\":295 -> \"295\"\n" +
                "\"322\":44 -> \"44\"\n" +
                "\"322\":45 -> \"45\"\n" +
                "\"322\":296 -> \"296\"\n" +
                "\"322\":lastChild -> \"70\"\n" +
                "\"293\"[label = \"\"];\n" +
                "\"294\"[label = \"\"];\n" +
                "\"295\"[label = \"\"];\n" +
                "\"44\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"45\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"296\"[label = \" <30> |30|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"70\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"347\"[label = \"{ log | { 60--1 |  61--1 |  62--1 |  63--1 |  64--1 |  65--1 |  66--1 |  67--1 |  68--1 |  69--1 |  70--1 |  71--1 |  72--1 |  73--1 |  74--1 |  75--1 |  76--1 |  77--1 |  78--1 }}| <349> |45|  <350> |50|  <351> |55|  <94> |65|  <95> |70|  <127> |75|  <lastChild> |Ls \"];\n" +
                "\"347\":349 -> \"349\"\n" +
                "\"347\":350 -> \"350\"\n" +
                "\"347\":351 -> \"351\"\n" +
                "\"347\":94 -> \"94\"\n" +
                "\"347\":95 -> \"95\"\n" +
                "\"347\":127 -> \"127\"\n" +
                "\"347\":lastChild -> \"352\"\n" +
                "\"349\"[label = \"\"];\n" +
                "\"350\"[label = \"\"];\n" +
                "\"351\"[label = \"\"];\n" +
                "\"94\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"95\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"127\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"352\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78| \"];\n" +
                "\"408\"[label = \"{ log | { 100--1 |  101--1 |  102--1 |  104--1 |  105--1 |  106--1 |  107--1 |  108--1 |  109--1 |  110--1 |  111--1 |  112--1 |  113--1 |  114--1 |  115--1 |  116--1 |  117--1 |  118--1 |  119--1 }}| <379> |85|  <380> |90|  <381> |95|  <382> |105|  <163> |110|  <193> |115|  <lastChild> |Ls \"];\n" +
                "\"408\":379 -> \"379\"\n" +
                "\"408\":380 -> \"380\"\n" +
                "\"408\":381 -> \"381\"\n" +
                "\"408\":382 -> \"382\"\n" +
                "\"408\":163 -> \"163\"\n" +
                "\"408\":193 -> \"193\"\n" +
                "\"408\":lastChild -> \"194\"\n" +
                "\"379\"[label = \"\"];\n" +
                "\"380\"[label = \"\"];\n" +
                "\"381\"[label = \"\"];\n" +
                "\"382\"[label = \" <100> |100|  <101> |101|  <102> |102|  <104> |104| \"];\n" +
                "\"163\"[label = \" <105> |105|  <106> |106|  <107> |107|  <108> |108|  <109> |109| \"];\n" +
                "\"193\"[label = \" <110> |110|  <111> |111|  <112> |112|  <113> |113|  <114> |114| \"];\n" +
                "\"194\"[label = \" <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"464\"[label = \"{ log | { 140--1 |  141--1 |  142--1 |  143--1 |  144--1 |  145--1 |  146--1 |  147--1 |  148--1 |  149--1 |  150--1 |  152--1 |  153--1 |  154--1 |  155--1 |  156--1 |  157--1 |  158--1 |  159--1 }}| <435> |125|  <436> |130|  <437> |135|  <199> |145|  <200> |150|  <438> |155|  <lastChild> |Ls \"];\n" +
                "\"464\":435 -> \"435\"\n" +
                "\"464\":436 -> \"436\"\n" +
                "\"464\":437 -> \"437\"\n" +
                "\"464\":199 -> \"199\"\n" +
                "\"464\":200 -> \"200\"\n" +
                "\"464\":438 -> \"438\"\n" +
                "\"464\":lastChild -> \"230\"\n" +
                "\"435\"[label = \"\"];\n" +
                "\"436\"[label = \"\"];\n" +
                "\"437\"[label = \"\"];\n" +
                "\"199\"[label = \" <140> |140|  <141> |141|  <142> |142|  <143> |143|  <144> |144| \"];\n" +
                "\"200\"[label = \" <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"438\"[label = \" <150> |150|  <152> |152|  <153> |153|  <154> |154| \"];\n" +
                "\"230\"[label = \" <155> |155|  <156> |156|  <157> |157|  <158> |158|  <159> |159| \"];\n" +
                "\"489\"[label = \"{ log | { 180--1 |  181--1 |  182--1 |  183--1 |  184--1 |  185--1 |  186--1 |  187--1 |  188--1 |  189--1 |  190--1 |  191--1 |  192--1 |  193--1 |  194--1 |  195--1 |  196--1 |  197--1 |  198--1 }}| <491> |165|  <492> |170|  <493> |175|  <263> |185|  <264> |190|  <lastChild> |Ls \"];\n" +
                "\"489\":491 -> \"491\"\n" +
                "\"489\":492 -> \"492\"\n" +
                "\"489\":493 -> \"493\"\n" +
                "\"489\":263 -> \"263\"\n" +
                "\"489\":264 -> \"264\"\n" +
                "\"489\":lastChild -> \"494\"\n" +
                "\"491\"[label = \"\"];\n" +
                "\"492\"[label = \"\"];\n" +
                "\"493\"[label = \"\"];\n" +
                "\"263\"[label = \" <180> |180|  <181> |181|  <182> |182|  <183> |183|  <184> |184| \"];\n" +
                "\"264\"[label = \" <185> |185|  <186> |186|  <187> |187|  <188> |188|  <189> |189| \"];\n" +
                "\"494\"[label = \" <190> |190|  <191> |191|  <192> |192|  <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197|  <198> |198| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
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

        //index is 0 based
        final long actual0 = bTree.get(key, 0);
        assertEquals(expectedValue1, actual0);

        final long actual1 = bTree.get(key, 1);
        assertEquals(expectedValue2, actual1);

        final long actual2 = bTree.get(key, 2);
        assertEquals(expectedValue3, actual2);

        try
        {
            bTree.get(key, 3);
            fail();
        }
        catch (final IllegalArgumentException e)
        {
            assertEquals("Didn't have version 3", e.getMessage());
        }

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
    void shouldConsumeKeyValuesInOrderForAGivenVersion()
    {
        final List<Long> expectedOrder = new ArrayList<>();
        final int version = 50;
        for (long i = 0; i < 100; i++)
        {
            if (i <= version)
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
                "\"250\"[label = \" <123> |30|  <220> |60|  <lastChild> |Ls \"];\n" +
                "\"250\":123 -> \"123\"\n" +
                "\"250\":220 -> \"220\"\n" +
                "\"250\":lastChild -> \"249\"\n" +
                "\"123\"[label = \" <11> |5|  <22> |10|  <33> |15|  <44> |20|  <55> |25|  <lastChild> |Ls \"];\n" +
                "\"123\":11 -> \"11\"\n" +
                "\"123\":22 -> \"22\"\n" +
                "\"123\":33 -> \"33\"\n" +
                "\"123\":44 -> \"44\"\n" +
                "\"123\":55 -> \"55\"\n" +
                "\"123\":lastChild -> \"66\"\n" +
                "\"11\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4| \"];\n" +
                "\"22\"[label = \" <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                "\"33\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14| \"];\n" +
                "\"44\"[label = \" <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                "\"55\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24| \"];\n" +
                "\"66\"[label = \" <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"220\"[label = \" <77> |35|  <88> |40|  <99> |45|  <110> |50|  <121> |55|  <lastChild> |Ls \"];\n" +
                "\"220\":77 -> \"77\"\n" +
                "\"220\":88 -> \"88\"\n" +
                "\"220\":99 -> \"99\"\n" +
                "\"220\":110 -> \"110\"\n" +
                "\"220\":121 -> \"121\"\n" +
                "\"220\":lastChild -> \"138\"\n" +
                "\"77\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34| \"];\n" +
                "\"88\"[label = \" <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                "\"99\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44| \"];\n" +
                "\"110\"[label = \" <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                "\"121\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54| \"];\n" +
                "\"138\"[label = \" <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"249\"[label = \" <154> |65|  <170> |70|  <186> |75|  <202> |80|  <218> |85|  <235> |90|  <lastChild> |Ls \"];\n" +
                "\"249\":154 -> \"154\"\n" +
                "\"249\":170 -> \"170\"\n" +
                "\"249\":186 -> \"186\"\n" +
                "\"249\":202 -> \"202\"\n" +
                "\"249\":218 -> \"218\"\n" +
                "\"249\":235 -> \"235\"\n" +
                "\"249\":lastChild -> \"248\"\n" +
                "\"154\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64| \"];\n" +
                "\"170\"[label = \" <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"186\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74| \"];\n" +
                "\"202\"[label = \" <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                "\"218\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84| \"];\n" +
                "\"235\"[label = \" <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"248\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}