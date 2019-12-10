package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;
import static org.logdb.integration.TestIntegrationUtils.createNewPersistedLogBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedLogBtree;

class BBTreeWithLogIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BBTreeWithLogIntegrationTest.class);

    @TempDir
    Path tempDirectory;

    @Test
    void shouldNotFindKeyLowerThatAnyOtherKey() throws Exception
    {
        final long nonExistingKeyValuePair = -1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.commit();
            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldNotFindKeyBiggerThatAnyOtherKey() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.commit();
            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldBeABleToCreateNewDBWithLogFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(1, 1);
            bTree.put(10, 10);
            bTree.put(5, 5);

            assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(tempDirectory))
        {
            assertEquals(1, readBTree.get(1));
            assertEquals(10, readBTree.get(10));
            assertEquals(5, readBTree.get(5));

            assertEquals(KEY_NOT_FOUND_VALUE, readBTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDBWithLog() throws Exception
    {
        final long key = 123L;
        final int maxVersions = 100;
        final long nonExistingKey = 964L;

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
                originalBTree.commit();
            }

            assertEquals(KEY_NOT_FOUND_VALUE, originalBTree.get(nonExistingKey));
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < maxVersions; i++)
            {
                assertEquals(i, loadedBTree.get(key, i));
            }

            assertEquals(KEY_NOT_FOUND_VALUE, loadedBTree.get(nonExistingKey));
        }
    }

    @Test
    void shouldGetHistoricValuesByTimestampFromOpenDB() throws Exception
    {
        final long key = 123L;
        final int maxVersions = 100;

        final StubTimeSource timeSource = new StubTimeSource();

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory, TestUtils.BYTE_ORDER, timeSource))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
            }

            originalBTree.commit();
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < timeSource.getCurrentTimeWithoutIncrementing(); i++)
            {
                assertEquals(i, loadedBTree.getByTimestamp(key, i));
            }
        }
    }

    @Test
    void shouldBeABleToCommitMultipleTimesWithLog() throws Exception
    {
        final int numberOfPairs = 600;

        final String original;
        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {

            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }

            original = originalBTree.print();
            originalBTree.commit();
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            final String loaded = loadedBTree.print();

            assertEquals(original, loaded);

            for (int i = 0; i < numberOfPairs; i++)
            {
                assertEquals(i, loadedBTree.get(i));
            }
        }
    }

    @Test
    void shouldBeABleToLoadAndContinuePersistingBtreeWithLog() throws Exception
    {
        final int numberOfPairs = 300;

        //create new tree and insert elements
        try (BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairs; i++)
            {
                originalBTree.put(i, i);
                originalBTree.commit();
            }
            originalBTree.commit();
        }

        //open the persisted tree and continue inserting elements
        try (BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = numberOfPairs; i < numberOfPairs * 2; i++)
            {
                loadedBTree.put(i, i);
                loadedBTree.commit();
            }
        }

        //open the persisted tree and read all the elements
        try (BTreeWithLog loadedBTree2 = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs * 2; i++)
            {
                assertEquals(i, loadedBTree2.get(i));
            }
        }
    }

    @Test
    void shouldBeABleInsertAndDeleteFromLoadedBtreeWithLog() throws Exception
    {
        final int numberOfPairsToInsert = 600;
        final int numberOfPairsToDelete = 100;
        final int startOffset = 350;
        final int endDeletionOffset = startOffset + numberOfPairsToDelete;

        try (BTreeWithLog originalTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairsToInsert; i++)
            {
                originalTree.put(i, i);
                originalTree.commit();
            }
            originalTree.commit();
        }


        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = startOffset; i < endDeletionOffset; i++)
            {
                loadedTree.remove(i);
                loadedTree.commit();
            }
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"875\"[label = \"{ log | { 449--1 }}| <327> |140|  <509> |280|  <846> |420|  <lastChild> |Ls \"];\n" +
                "\"875\":327 -> \"327\"\n" +
                "\"875\":509 -> \"509\"\n" +
                "\"875\":846 -> \"846\"\n" +
                "\"875\":lastChild -> \"873\"\n" +
                "\"327\"[label = \" <31> |14|  <54> |28|  <66> |42|  <89> |56|  <101> |70|  <113> |84|  <136> |98|  <148> |112|  <171> |126|  <lastChild> |Ls \"];\n" +
                "\"327\":31 -> \"31\"\n" +
                "\"327\":54 -> \"54\"\n" +
                "\"327\":66 -> \"66\"\n" +
                "\"327\":89 -> \"89\"\n" +
                "\"327\":101 -> \"101\"\n" +
                "\"327\":113 -> \"113\"\n" +
                "\"327\":136 -> \"136\"\n" +
                "\"327\":148 -> \"148\"\n" +
                "\"327\":171 -> \"171\"\n" +
                "\"327\":lastChild -> \"183\"\n" +
                "\"31\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11|  <12> |12|  <13> |13| \"];\n" +
                "\"54\"[label = \" <14> |14|  <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24|  <25> |25|  <26> |26|  <27> |27| \"];\n" +
                "\"66\"[label = \" <28> |28|  <29> |29|  <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"89\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54|  <55> |55| \"];\n" +
                "\"101\"[label = \" <56> |56|  <57> |57|  <58> |58|  <59> |59|  <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                "\"113\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"136\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89|  <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97| \"];\n" +
                "\"148\"[label = \" <98> |98|  <99> |99|  <100> |100|  <101> |101|  <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107|  <108> |108|  <109> |109|  <110> |110|  <111> |111| \"];\n" +
                "\"171\"[label = \" <112> |112|  <113> |113|  <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119|  <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125| \"];\n" +
                "\"183\"[label = \" <126> |126|  <127> |127|  <128> |128|  <129> |129|  <130> |130|  <131> |131|  <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137|  <138> |138|  <139> |139| \"];\n" +
                "\"509\"[label = \" <195> |154|  <221> |168|  <233> |182|  <256> |196|  <268> |210|  <280> |224|  <303> |238|  <315> |252|  <340> |266|  <lastChild> |Ls \"];\n" +
                "\"509\":195 -> \"195\"\n" +
                "\"509\":221 -> \"221\"\n" +
                "\"509\":233 -> \"233\"\n" +
                "\"509\":256 -> \"256\"\n" +
                "\"509\":268 -> \"268\"\n" +
                "\"509\":280 -> \"280\"\n" +
                "\"509\":303 -> \"303\"\n" +
                "\"509\":315 -> \"315\"\n" +
                "\"509\":340 -> \"340\"\n" +
                "\"509\":lastChild -> \"353\"\n" +
                "\"195\"[label = \" <140> |140|  <141> |141|  <142> |142|  <143> |143|  <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149|  <150> |150|  <151> |151|  <152> |152|  <153> |153| \"];\n" +
                "\"221\"[label = \" <154> |154|  <155> |155|  <156> |156|  <157> |157|  <158> |158|  <159> |159|  <160> |160|  <161> |161|  <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167| \"];\n" +
                "\"233\"[label = \" <168> |168|  <169> |169|  <170> |170|  <171> |171|  <172> |172|  <173> |173|  <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179|  <180> |180|  <181> |181| \"];\n" +
                "\"256\"[label = \" <182> |182|  <183> |183|  <184> |184|  <185> |185|  <186> |186|  <187> |187|  <188> |188|  <189> |189|  <190> |190|  <191> |191|  <192> |192|  <193> |193|  <194> |194|  <195> |195| \"];\n" +
                "\"268\"[label = \" <196> |196|  <197> |197|  <198> |198|  <199> |199|  <200> |200|  <201> |201|  <202> |202|  <203> |203|  <204> |204|  <205> |205|  <206> |206|  <207> |207|  <208> |208|  <209> |209| \"];\n" +
                "\"280\"[label = \" <210> |210|  <211> |211|  <212> |212|  <213> |213|  <214> |214|  <215> |215|  <216> |216|  <217> |217|  <218> |218|  <219> |219|  <220> |220|  <221> |221|  <222> |222|  <223> |223| \"];\n" +
                "\"303\"[label = \" <224> |224|  <225> |225|  <226> |226|  <227> |227|  <228> |228|  <229> |229|  <230> |230|  <231> |231|  <232> |232|  <233> |233|  <234> |234|  <235> |235|  <236> |236|  <237> |237| \"];\n" +
                "\"315\"[label = \" <238> |238|  <239> |239|  <240> |240|  <241> |241|  <242> |242|  <243> |243|  <244> |244|  <245> |245|  <246> |246|  <247> |247|  <248> |248|  <249> |249|  <250> |250|  <251> |251| \"];\n" +
                "\"340\"[label = \" <252> |252|  <253> |253|  <254> |254|  <255> |255|  <256> |256|  <257> |257|  <258> |258|  <259> |259|  <260> |260|  <261> |261|  <262> |262|  <263> |263|  <264> |264|  <265> |265| \"];\n" +
                "\"353\"[label = \" <266> |266|  <267> |267|  <268> |268|  <269> |269|  <270> |270|  <271> |271|  <272> |272|  <273> |273|  <274> |274|  <275> |275|  <276> |276|  <277> |277|  <278> |278|  <279> |279| \"];\n" +
                "\"846\"[label = \" <366> |294|  <391> |308|  <407> |322|  <432> |336|  <lastChild> |Ls \"];\n" +
                "\"846\":366 -> \"366\"\n" +
                "\"846\":391 -> \"391\"\n" +
                "\"846\":407 -> \"407\"\n" +
                "\"846\":432 -> \"432\"\n" +
                "\"846\":lastChild -> \"445\"\n" +
                "\"366\"[label = \" <280> |280|  <281> |281|  <282> |282|  <283> |283|  <284> |284|  <285> |285|  <286> |286|  <287> |287|  <288> |288|  <289> |289|  <290> |290|  <291> |291|  <292> |292|  <293> |293| \"];\n" +
                "\"391\"[label = \" <294> |294|  <295> |295|  <296> |296|  <297> |297|  <298> |298|  <299> |299|  <300> |300|  <301> |301|  <302> |302|  <303> |303|  <304> |304|  <305> |305|  <306> |306|  <307> |307| \"];\n" +
                "\"407\"[label = \" <308> |308|  <309> |309|  <310> |310|  <311> |311|  <312> |312|  <313> |313|  <314> |314|  <315> |315|  <316> |316|  <317> |317|  <318> |318|  <319> |319|  <320> |320|  <321> |321| \"];\n" +
                "\"432\"[label = \" <322> |322|  <323> |323|  <324> |324|  <325> |325|  <326> |326|  <327> |327|  <328> |328|  <329> |329|  <330> |330|  <331> |331|  <332> |332|  <333> |333|  <334> |334|  <335> |335| \"];\n" +
                "\"445\"[label = \" <336> |336|  <337> |337|  <338> |338|  <339> |339|  <340> |340|  <341> |341|  <342> |342|  <343> |343|  <344> |344|  <345> |345|  <346> |346|  <347> |347|  <348> |348|  <349> |349| \"];\n" +
                "\"873\"[label = \" <872> |462|  <614> |476|  <627> |490|  <640> |504|  <665> |518|  <678> |532|  <704> |546|  <717> |560|  <730> |574|  <lastChild> |Ls \"];\n" +
                "\"873\":872 -> \"872\"\n" +
                "\"873\":614 -> \"614\"\n" +
                "\"873\":627 -> \"627\"\n" +
                "\"873\":640 -> \"640\"\n" +
                "\"873\":665 -> \"665\"\n" +
                "\"873\":678 -> \"678\"\n" +
                "\"873\":704 -> \"704\"\n" +
                "\"873\":717 -> \"717\"\n" +
                "\"873\":730 -> \"730\"\n" +
                "\"873\":lastChild -> \"848\"\n" +
                "\"872\"[label = \" <449> |449|  <450> |450|  <451> |451|  <452> |452|  <453> |453|  <454> |454|  <455> |455|  <456> |456|  <457> |457|  <458> |458|  <459> |459|  <460> |460|  <461> |461| \"];\n" +
                "\"614\"[label = \" <462> |462|  <463> |463|  <464> |464|  <465> |465|  <466> |466|  <467> |467|  <468> |468|  <469> |469|  <470> |470|  <471> |471|  <472> |472|  <473> |473|  <474> |474|  <475> |475| \"];\n" +
                "\"627\"[label = \" <476> |476|  <477> |477|  <478> |478|  <479> |479|  <480> |480|  <481> |481|  <482> |482|  <483> |483|  <484> |484|  <485> |485|  <486> |486|  <487> |487|  <488> |488|  <489> |489| \"];\n" +
                "\"640\"[label = \" <490> |490|  <491> |491|  <492> |492|  <493> |493|  <494> |494|  <495> |495|  <496> |496|  <497> |497|  <498> |498|  <499> |499|  <500> |500|  <501> |501|  <502> |502|  <503> |503| \"];\n" +
                "\"665\"[label = \" <504> |504|  <505> |505|  <506> |506|  <507> |507|  <508> |508|  <509> |509|  <510> |510|  <511> |511|  <512> |512|  <513> |513|  <514> |514|  <515> |515|  <516> |516|  <517> |517| \"];\n" +
                "\"678\"[label = \" <518> |518|  <519> |519|  <520> |520|  <521> |521|  <522> |522|  <523> |523|  <524> |524|  <525> |525|  <526> |526|  <527> |527|  <528> |528|  <529> |529|  <530> |530|  <531> |531| \"];\n" +
                "\"704\"[label = \" <532> |532|  <533> |533|  <534> |534|  <535> |535|  <536> |536|  <537> |537|  <538> |538|  <539> |539|  <540> |540|  <541> |541|  <542> |542|  <543> |543|  <544> |544|  <545> |545| \"];\n" +
                "\"717\"[label = \" <546> |546|  <547> |547|  <548> |548|  <549> |549|  <550> |550|  <551> |551|  <552> |552|  <553> |553|  <554> |554|  <555> |555|  <556> |556|  <557> |557|  <558> |558|  <559> |559| \"];\n" +
                "\"730\"[label = \" <560> |560|  <561> |561|  <562> |562|  <563> |563|  <564> |564|  <565> |565|  <566> |566|  <567> |567|  <568> |568|  <569> |569|  <570> |570|  <571> |571|  <572> |572|  <573> |573| \"];\n" +
                "\"848\"[label = \" <574> |574|  <575> |575|  <576> |576|  <577> |577|  <578> |578|  <579> |579|  <580> |580|  <581> |581|  <582> |582|  <583> |583|  <584> |584|  <585> |585|  <586> |586|  <587> |587|  <588> |588|  <589> |589|  <590> |590|  <591> |591|  <592> |592|  <593> |593|  <594> |594|  <595> |595|  <596> |596|  <597> |597|  <598> |598|  <599> |599| \"];\n" +
                "}\n";

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairsToInsert; i++)
            {
                //delete range,don't assert on it
                if (i >= startOffset && i < endDeletionOffset)
                {
                    assertEquals(KEY_NOT_FOUND_VALUE, loadedTree.get(i));
                }
                else
                {
                    assertEquals(i, loadedTree.get(i));
                }
            }

            assertEquals(expectedTree, loadedTree.print());
        }
    }

    @Test
    void shouldBeABleInsertAndDeleteAndInsertSomeMoreInBtreeWithLog() throws Exception
    {
        final int numberOfPairsToInsert = 600;
        final int numberOfPairsToDelete = 100;
        final int startOffset = 350;
        final int endDeletionOffset = startOffset + numberOfPairsToDelete;

        try (BTreeWithLog newTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairsToInsert; i++)
            {
                newTree.put(i, i);
                newTree.commit();
            }
            newTree.commit();
        }


        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = startOffset; i < endDeletionOffset; i++)
            {
                loadedTree.remove(i);
                loadedTree.commit();
            }
        }

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = startOffset; i < endDeletionOffset; i++)
            {
                loadedTree.put(i, i);
                loadedTree.commit();
            }
        }

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairsToInsert; i++)
            {
                assertEquals(i, loadedTree.get(i));
            }
        }
    }
}
