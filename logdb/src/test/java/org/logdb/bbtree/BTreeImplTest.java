package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.createInitialRootReference;

class BTreeImplTest
{
    private static final int PAGE_SIZE = 256;
    private BTreeImpl bTree;

    @BeforeEach
    void setUp()
    {
        final Storage storage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE);

        final NodesManager nodesManager = new NodesManager(storage);

        bTree = new BTreeImpl(
                nodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager));
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

        //index for history is 0 based
        assertEquals(expectedValue1, bTree.get(key, 0));
        assertEquals(expectedValue2, bTree.get(key, 1));
        assertEquals(expectedValue3, bTree.get(key, 2));

        try
        {
            bTree.get(key, 3);
            fail();
        }
        catch (final VersionNotFoundException e)
        {
            assertEquals("The version 3 was not found.", e.getMessage());
        }

        //get latest version by default
        final long actualLatest = bTree.get(key);
        assertEquals(expectedValue3, actualLatest);
    }

    @Test
    void shouldConsumeKeyValuesInOrder()
    {
        final List<Long> expectedOrder = new ArrayList<>();
        for (long i = 0; i < 500; i++)
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
        final int version = 400;
        for (long i = 0; i < 500; i++)
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
        final int size = 600;
        for (int i = 0; i < size; i++)
        {
            bTree.put(i, i);
        }

        for (int i = 0; i < size; i++)
        {
            bTree.remove(i);
        }

        assertEquals(1L, bTree.getNodesCount());
    }

    @Test
    void shouldBeAbleToInsertElements()
    {
        for (int i = 0; i < 600; i++)
        {
            bTree.put(i, i);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"2002\"[label = \" <1296> |216|  <lastChild> |Ls \"];\n" +
                "\"2002\":1296 -> \"1296\"\n" +
                "\"2002\":lastChild -> \"2001\"\n" +
                "\"1296\"[label = \" <144> |36|  <259> |72|  <374> |108|  <489> |144|  <604> |180|  <lastChild> |Ls \"];\n" +
                "\"1296\":144 -> \"144\"\n" +
                "\"1296\":259 -> \"259\"\n" +
                "\"1296\":374 -> \"374\"\n" +
                "\"1296\":489 -> \"489\"\n" +
                "\"1296\":604 -> \"604\"\n" +
                "\"1296\":lastChild -> \"719\"\n" +
                "\"144\"[label = \" <12> |6|  <25> |12|  <38> |18|  <51> |24|  <64> |30|  <lastChild> |Ls \"];\n" +
                "\"144\":12 -> \"12\"\n" +
                "\"144\":25 -> \"25\"\n" +
                "\"144\":38 -> \"38\"\n" +
                "\"144\":51 -> \"51\"\n" +
                "\"144\":64 -> \"64\"\n" +
                "\"144\":lastChild -> \"77\"\n" +
                "\"12\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5| \"];\n" +
                "\"25\"[label = \" <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11| \"];\n" +
                "\"38\"[label = \" <12> |12|  <13> |13|  <14> |14|  <15> |15|  <16> |16|  <17> |17| \"];\n" +
                "\"51\"[label = \" <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23| \"];\n" +
                "\"64\"[label = \" <24> |24|  <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"77\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35| \"];\n" +
                "\"259\"[label = \" <90> |42|  <103> |48|  <116> |54|  <129> |60|  <142> |66|  <lastChild> |Ls \"];\n" +
                "\"259\":90 -> \"90\"\n" +
                "\"259\":103 -> \"103\"\n" +
                "\"259\":116 -> \"116\"\n" +
                "\"259\":129 -> \"129\"\n" +
                "\"259\":142 -> \"142\"\n" +
                "\"259\":lastChild -> \"162\"\n" +
                "\"90\"[label = \" <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"103\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47| \"];\n" +
                "\"116\"[label = \" <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53| \"];\n" +
                "\"129\"[label = \" <54> |54|  <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"142\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65| \"];\n" +
                "\"162\"[label = \" <66> |66|  <67> |67|  <68> |68|  <69> |69|  <70> |70|  <71> |71| \"];\n" +
                "\"374\"[label = \" <181> |78|  <200> |84|  <219> |90|  <238> |96|  <257> |102|  <lastChild> |Ls \"];\n" +
                "\"374\":181 -> \"181\"\n" +
                "\"374\":200 -> \"200\"\n" +
                "\"374\":219 -> \"219\"\n" +
                "\"374\":238 -> \"238\"\n" +
                "\"374\":257 -> \"257\"\n" +
                "\"374\":lastChild -> \"277\"\n" +
                "\"181\"[label = \" <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77| \"];\n" +
                "\"200\"[label = \" <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"219\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"238\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95| \"];\n" +
                "\"257\"[label = \" <96> |96|  <97> |97|  <98> |98|  <99> |99|  <100> |100|  <101> |101| \"];\n" +
                "\"277\"[label = \" <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107| \"];\n" +
                "\"489\"[label = \" <296> |114|  <315> |120|  <334> |126|  <353> |132|  <372> |138|  <lastChild> |Ls \"];\n" +
                "\"489\":296 -> \"296\"\n" +
                "\"489\":315 -> \"315\"\n" +
                "\"489\":334 -> \"334\"\n" +
                "\"489\":353 -> \"353\"\n" +
                "\"489\":372 -> \"372\"\n" +
                "\"489\":lastChild -> \"392\"\n" +
                "\"296\"[label = \" <108> |108|  <109> |109|  <110> |110|  <111> |111|  <112> |112|  <113> |113| \"];\n" +
                "\"315\"[label = \" <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"334\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125| \"];\n" +
                "\"353\"[label = \" <126> |126|  <127> |127|  <128> |128|  <129> |129|  <130> |130|  <131> |131| \"];\n" +
                "\"372\"[label = \" <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137| \"];\n" +
                "\"392\"[label = \" <138> |138|  <139> |139|  <140> |140|  <141> |141|  <142> |142|  <143> |143| \"];\n" +
                "\"604\"[label = \" <411> |150|  <430> |156|  <449> |162|  <468> |168|  <487> |174|  <lastChild> |Ls \"];\n" +
                "\"604\":411 -> \"411\"\n" +
                "\"604\":430 -> \"430\"\n" +
                "\"604\":449 -> \"449\"\n" +
                "\"604\":468 -> \"468\"\n" +
                "\"604\":487 -> \"487\"\n" +
                "\"604\":lastChild -> \"507\"\n" +
                "\"411\"[label = \" <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"430\"[label = \" <150> |150|  <151> |151|  <152> |152|  <153> |153|  <154> |154|  <155> |155| \"];\n" +
                "\"449\"[label = \" <156> |156|  <157> |157|  <158> |158|  <159> |159|  <160> |160|  <161> |161| \"];\n" +
                "\"468\"[label = \" <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167| \"];\n" +
                "\"487\"[label = \" <168> |168|  <169> |169|  <170> |170|  <171> |171|  <172> |172|  <173> |173| \"];\n" +
                "\"507\"[label = \" <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179| \"];\n" +
                "\"719\"[label = \" <526> |186|  <545> |192|  <564> |198|  <583> |204|  <602> |210|  <lastChild> |Ls \"];\n" +
                "\"719\":526 -> \"526\"\n" +
                "\"719\":545 -> \"545\"\n" +
                "\"719\":564 -> \"564\"\n" +
                "\"719\":583 -> \"583\"\n" +
                "\"719\":602 -> \"602\"\n" +
                "\"719\":lastChild -> \"622\"\n" +
                "\"526\"[label = \" <180> |180|  <181> |181|  <182> |182|  <183> |183|  <184> |184|  <185> |185| \"];\n" +
                "\"545\"[label = \" <186> |186|  <187> |187|  <188> |188|  <189> |189|  <190> |190|  <191> |191| \"];\n" +
                "\"564\"[label = \" <192> |192|  <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197| \"];\n" +
                "\"583\"[label = \" <198> |198|  <199> |199|  <200> |200|  <201> |201|  <202> |202|  <203> |203| \"];\n" +
                "\"602\"[label = \" <204> |204|  <205> |205|  <206> |206|  <207> |207|  <208> |208|  <209> |209| \"];\n" +
                "\"622\"[label = \" <210> |210|  <211> |211|  <212> |212|  <213> |213|  <214> |214|  <215> |215| \"];\n" +
                "\"2001\"[label = \" <834> |252|  <949> |288|  <1064> |324|  <1179> |360|  <1294> |396|  <1446> |432|  <1597> |468|  <1748> |504|  <1899> |540|  <lastChild> |Ls \"];\n" +
                "\"2001\":834 -> \"834\"\n" +
                "\"2001\":949 -> \"949\"\n" +
                "\"2001\":1064 -> \"1064\"\n" +
                "\"2001\":1179 -> \"1179\"\n" +
                "\"2001\":1294 -> \"1294\"\n" +
                "\"2001\":1446 -> \"1446\"\n" +
                "\"2001\":1597 -> \"1597\"\n" +
                "\"2001\":1748 -> \"1748\"\n" +
                "\"2001\":1899 -> \"1899\"\n" +
                "\"2001\":lastChild -> \"2000\"\n" +
                "\"834\"[label = \" <641> |222|  <660> |228|  <679> |234|  <698> |240|  <717> |246|  <lastChild> |Ls \"];\n" +
                "\"834\":641 -> \"641\"\n" +
                "\"834\":660 -> \"660\"\n" +
                "\"834\":679 -> \"679\"\n" +
                "\"834\":698 -> \"698\"\n" +
                "\"834\":717 -> \"717\"\n" +
                "\"834\":lastChild -> \"737\"\n" +
                "\"641\"[label = \" <216> |216|  <217> |217|  <218> |218|  <219> |219|  <220> |220|  <221> |221| \"];\n" +
                "\"660\"[label = \" <222> |222|  <223> |223|  <224> |224|  <225> |225|  <226> |226|  <227> |227| \"];\n" +
                "\"679\"[label = \" <228> |228|  <229> |229|  <230> |230|  <231> |231|  <232> |232|  <233> |233| \"];\n" +
                "\"698\"[label = \" <234> |234|  <235> |235|  <236> |236|  <237> |237|  <238> |238|  <239> |239| \"];\n" +
                "\"717\"[label = \" <240> |240|  <241> |241|  <242> |242|  <243> |243|  <244> |244|  <245> |245| \"];\n" +
                "\"737\"[label = \" <246> |246|  <247> |247|  <248> |248|  <249> |249|  <250> |250|  <251> |251| \"];\n" +
                "\"949\"[label = \" <756> |258|  <775> |264|  <794> |270|  <813> |276|  <832> |282|  <lastChild> |Ls \"];\n" +
                "\"949\":756 -> \"756\"\n" +
                "\"949\":775 -> \"775\"\n" +
                "\"949\":794 -> \"794\"\n" +
                "\"949\":813 -> \"813\"\n" +
                "\"949\":832 -> \"832\"\n" +
                "\"949\":lastChild -> \"852\"\n" +
                "\"756\"[label = \" <252> |252|  <253> |253|  <254> |254|  <255> |255|  <256> |256|  <257> |257| \"];\n" +
                "\"775\"[label = \" <258> |258|  <259> |259|  <260> |260|  <261> |261|  <262> |262|  <263> |263| \"];\n" +
                "\"794\"[label = \" <264> |264|  <265> |265|  <266> |266|  <267> |267|  <268> |268|  <269> |269| \"];\n" +
                "\"813\"[label = \" <270> |270|  <271> |271|  <272> |272|  <273> |273|  <274> |274|  <275> |275| \"];\n" +
                "\"832\"[label = \" <276> |276|  <277> |277|  <278> |278|  <279> |279|  <280> |280|  <281> |281| \"];\n" +
                "\"852\"[label = \" <282> |282|  <283> |283|  <284> |284|  <285> |285|  <286> |286|  <287> |287| \"];\n" +
                "\"1064\"[label = \" <871> |294|  <890> |300|  <909> |306|  <928> |312|  <947> |318|  <lastChild> |Ls \"];\n" +
                "\"1064\":871 -> \"871\"\n" +
                "\"1064\":890 -> \"890\"\n" +
                "\"1064\":909 -> \"909\"\n" +
                "\"1064\":928 -> \"928\"\n" +
                "\"1064\":947 -> \"947\"\n" +
                "\"1064\":lastChild -> \"967\"\n" +
                "\"871\"[label = \" <288> |288|  <289> |289|  <290> |290|  <291> |291|  <292> |292|  <293> |293| \"];\n" +
                "\"890\"[label = \" <294> |294|  <295> |295|  <296> |296|  <297> |297|  <298> |298|  <299> |299| \"];\n" +
                "\"909\"[label = \" <300> |300|  <301> |301|  <302> |302|  <303> |303|  <304> |304|  <305> |305| \"];\n" +
                "\"928\"[label = \" <306> |306|  <307> |307|  <308> |308|  <309> |309|  <310> |310|  <311> |311| \"];\n" +
                "\"947\"[label = \" <312> |312|  <313> |313|  <314> |314|  <315> |315|  <316> |316|  <317> |317| \"];\n" +
                "\"967\"[label = \" <318> |318|  <319> |319|  <320> |320|  <321> |321|  <322> |322|  <323> |323| \"];\n" +
                "\"1179\"[label = \" <986> |330|  <1005> |336|  <1024> |342|  <1043> |348|  <1062> |354|  <lastChild> |Ls \"];\n" +
                "\"1179\":986 -> \"986\"\n" +
                "\"1179\":1005 -> \"1005\"\n" +
                "\"1179\":1024 -> \"1024\"\n" +
                "\"1179\":1043 -> \"1043\"\n" +
                "\"1179\":1062 -> \"1062\"\n" +
                "\"1179\":lastChild -> \"1082\"\n" +
                "\"986\"[label = \" <324> |324|  <325> |325|  <326> |326|  <327> |327|  <328> |328|  <329> |329| \"];\n" +
                "\"1005\"[label = \" <330> |330|  <331> |331|  <332> |332|  <333> |333|  <334> |334|  <335> |335| \"];\n" +
                "\"1024\"[label = \" <336> |336|  <337> |337|  <338> |338|  <339> |339|  <340> |340|  <341> |341| \"];\n" +
                "\"1043\"[label = \" <342> |342|  <343> |343|  <344> |344|  <345> |345|  <346> |346|  <347> |347| \"];\n" +
                "\"1062\"[label = \" <348> |348|  <349> |349|  <350> |350|  <351> |351|  <352> |352|  <353> |353| \"];\n" +
                "\"1082\"[label = \" <354> |354|  <355> |355|  <356> |356|  <357> |357|  <358> |358|  <359> |359| \"];\n" +
                "\"1294\"[label = \" <1101> |366|  <1120> |372|  <1139> |378|  <1158> |384|  <1177> |390|  <lastChild> |Ls \"];\n" +
                "\"1294\":1101 -> \"1101\"\n" +
                "\"1294\":1120 -> \"1120\"\n" +
                "\"1294\":1139 -> \"1139\"\n" +
                "\"1294\":1158 -> \"1158\"\n" +
                "\"1294\":1177 -> \"1177\"\n" +
                "\"1294\":lastChild -> \"1197\"\n" +
                "\"1101\"[label = \" <360> |360|  <361> |361|  <362> |362|  <363> |363|  <364> |364|  <365> |365| \"];\n" +
                "\"1120\"[label = \" <366> |366|  <367> |367|  <368> |368|  <369> |369|  <370> |370|  <371> |371| \"];\n" +
                "\"1139\"[label = \" <372> |372|  <373> |373|  <374> |374|  <375> |375|  <376> |376|  <377> |377| \"];\n" +
                "\"1158\"[label = \" <378> |378|  <379> |379|  <380> |380|  <381> |381|  <382> |382|  <383> |383| \"];\n" +
                "\"1177\"[label = \" <384> |384|  <385> |385|  <386> |386|  <387> |387|  <388> |388|  <389> |389| \"];\n" +
                "\"1197\"[label = \" <390> |390|  <391> |391|  <392> |392|  <393> |393|  <394> |394|  <395> |395| \"];\n" +
                "\"1446\"[label = \" <1216> |402|  <1235> |408|  <1254> |414|  <1273> |420|  <1292> |426|  <lastChild> |Ls \"];\n" +
                "\"1446\":1216 -> \"1216\"\n" +
                "\"1446\":1235 -> \"1235\"\n" +
                "\"1446\":1254 -> \"1254\"\n" +
                "\"1446\":1273 -> \"1273\"\n" +
                "\"1446\":1292 -> \"1292\"\n" +
                "\"1446\":lastChild -> \"1319\"\n" +
                "\"1216\"[label = \" <396> |396|  <397> |397|  <398> |398|  <399> |399|  <400> |400|  <401> |401| \"];\n" +
                "\"1235\"[label = \" <402> |402|  <403> |403|  <404> |404|  <405> |405|  <406> |406|  <407> |407| \"];\n" +
                "\"1254\"[label = \" <408> |408|  <409> |409|  <410> |410|  <411> |411|  <412> |412|  <413> |413| \"];\n" +
                "\"1273\"[label = \" <414> |414|  <415> |415|  <416> |416|  <417> |417|  <418> |418|  <419> |419| \"];\n" +
                "\"1292\"[label = \" <420> |420|  <421> |421|  <422> |422|  <423> |423|  <424> |424|  <425> |425| \"];\n" +
                "\"1319\"[label = \" <426> |426|  <427> |427|  <428> |428|  <429> |429|  <430> |430|  <431> |431| \"];\n" +
                "\"1597\"[label = \" <1344> |438|  <1369> |444|  <1394> |450|  <1419> |456|  <1444> |462|  <lastChild> |Ls \"];\n" +
                "\"1597\":1344 -> \"1344\"\n" +
                "\"1597\":1369 -> \"1369\"\n" +
                "\"1597\":1394 -> \"1394\"\n" +
                "\"1597\":1419 -> \"1419\"\n" +
                "\"1597\":1444 -> \"1444\"\n" +
                "\"1597\":lastChild -> \"1470\"\n" +
                "\"1344\"[label = \" <432> |432|  <433> |433|  <434> |434|  <435> |435|  <436> |436|  <437> |437| \"];\n" +
                "\"1369\"[label = \" <438> |438|  <439> |439|  <440> |440|  <441> |441|  <442> |442|  <443> |443| \"];\n" +
                "\"1394\"[label = \" <444> |444|  <445> |445|  <446> |446|  <447> |447|  <448> |448|  <449> |449| \"];\n" +
                "\"1419\"[label = \" <450> |450|  <451> |451|  <452> |452|  <453> |453|  <454> |454|  <455> |455| \"];\n" +
                "\"1444\"[label = \" <456> |456|  <457> |457|  <458> |458|  <459> |459|  <460> |460|  <461> |461| \"];\n" +
                "\"1470\"[label = \" <462> |462|  <463> |463|  <464> |464|  <465> |465|  <466> |466|  <467> |467| \"];\n" +
                "\"1748\"[label = \" <1495> |474|  <1520> |480|  <1545> |486|  <1570> |492|  <1595> |498|  <lastChild> |Ls \"];\n" +
                "\"1748\":1495 -> \"1495\"\n" +
                "\"1748\":1520 -> \"1520\"\n" +
                "\"1748\":1545 -> \"1545\"\n" +
                "\"1748\":1570 -> \"1570\"\n" +
                "\"1748\":1595 -> \"1595\"\n" +
                "\"1748\":lastChild -> \"1621\"\n" +
                "\"1495\"[label = \" <468> |468|  <469> |469|  <470> |470|  <471> |471|  <472> |472|  <473> |473| \"];\n" +
                "\"1520\"[label = \" <474> |474|  <475> |475|  <476> |476|  <477> |477|  <478> |478|  <479> |479| \"];\n" +
                "\"1545\"[label = \" <480> |480|  <481> |481|  <482> |482|  <483> |483|  <484> |484|  <485> |485| \"];\n" +
                "\"1570\"[label = \" <486> |486|  <487> |487|  <488> |488|  <489> |489|  <490> |490|  <491> |491| \"];\n" +
                "\"1595\"[label = \" <492> |492|  <493> |493|  <494> |494|  <495> |495|  <496> |496|  <497> |497| \"];\n" +
                "\"1621\"[label = \" <498> |498|  <499> |499|  <500> |500|  <501> |501|  <502> |502|  <503> |503| \"];\n" +
                "\"1899\"[label = \" <1646> |510|  <1671> |516|  <1696> |522|  <1721> |528|  <1746> |534|  <lastChild> |Ls \"];\n" +
                "\"1899\":1646 -> \"1646\"\n" +
                "\"1899\":1671 -> \"1671\"\n" +
                "\"1899\":1696 -> \"1696\"\n" +
                "\"1899\":1721 -> \"1721\"\n" +
                "\"1899\":1746 -> \"1746\"\n" +
                "\"1899\":lastChild -> \"1772\"\n" +
                "\"1646\"[label = \" <504> |504|  <505> |505|  <506> |506|  <507> |507|  <508> |508|  <509> |509| \"];\n" +
                "\"1671\"[label = \" <510> |510|  <511> |511|  <512> |512|  <513> |513|  <514> |514|  <515> |515| \"];\n" +
                "\"1696\"[label = \" <516> |516|  <517> |517|  <518> |518|  <519> |519|  <520> |520|  <521> |521| \"];\n" +
                "\"1721\"[label = \" <522> |522|  <523> |523|  <524> |524|  <525> |525|  <526> |526|  <527> |527| \"];\n" +
                "\"1746\"[label = \" <528> |528|  <529> |529|  <530> |530|  <531> |531|  <532> |532|  <533> |533| \"];\n" +
                "\"1772\"[label = \" <534> |534|  <535> |535|  <536> |536|  <537> |537|  <538> |538|  <539> |539| \"];\n" +
                "\"2000\"[label = \" <1797> |546|  <1822> |552|  <1847> |558|  <1872> |564|  <1897> |570|  <1923> |576|  <1948> |582|  <1973> |588|  <1998> |594|  <lastChild> |Ls \"];\n" +
                "\"2000\":1797 -> \"1797\"\n" +
                "\"2000\":1822 -> \"1822\"\n" +
                "\"2000\":1847 -> \"1847\"\n" +
                "\"2000\":1872 -> \"1872\"\n" +
                "\"2000\":1897 -> \"1897\"\n" +
                "\"2000\":1923 -> \"1923\"\n" +
                "\"2000\":1948 -> \"1948\"\n" +
                "\"2000\":1973 -> \"1973\"\n" +
                "\"2000\":1998 -> \"1998\"\n" +
                "\"2000\":lastChild -> \"1999\"\n" +
                "\"1797\"[label = \" <540> |540|  <541> |541|  <542> |542|  <543> |543|  <544> |544|  <545> |545| \"];\n" +
                "\"1822\"[label = \" <546> |546|  <547> |547|  <548> |548|  <549> |549|  <550> |550|  <551> |551| \"];\n" +
                "\"1847\"[label = \" <552> |552|  <553> |553|  <554> |554|  <555> |555|  <556> |556|  <557> |557| \"];\n" +
                "\"1872\"[label = \" <558> |558|  <559> |559|  <560> |560|  <561> |561|  <562> |562|  <563> |563| \"];\n" +
                "\"1897\"[label = \" <564> |564|  <565> |565|  <566> |566|  <567> |567|  <568> |568|  <569> |569| \"];\n" +
                "\"1923\"[label = \" <570> |570|  <571> |571|  <572> |572|  <573> |573|  <574> |574|  <575> |575| \"];\n" +
                "\"1948\"[label = \" <576> |576|  <577> |577|  <578> |578|  <579> |579|  <580> |580|  <581> |581| \"];\n" +
                "\"1973\"[label = \" <582> |582|  <583> |583|  <584> |584|  <585> |585|  <586> |586|  <587> |587| \"];\n" +
                "\"1998\"[label = \" <588> |588|  <589> |589|  <590> |590|  <591> |591|  <592> |592|  <593> |593| \"];\n" +
                "\"1999\"[label = \" <594> |594|  <595> |595|  <596> |596|  <597> |597|  <598> |598|  <599> |599| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}