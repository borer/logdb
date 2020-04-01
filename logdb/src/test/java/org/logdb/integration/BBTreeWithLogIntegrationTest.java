package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bit.BinaryHelper;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        final byte[] nonExistingKeyValuePair = BinaryHelper.longToBytes( -1919191919L);

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            final byte[] bytes = BinaryHelper.longToBytes(1);
            bTree.put(bytes, bytes);
            bTree.commit();
            assertArrayEquals(null, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldNotFindKeyBiggerThatAnyOtherKey() throws Exception
    {
        final byte[] nonExistingKeyValuePair = BinaryHelper.longToBytes(1919191919L);

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            final byte[] bytes = BinaryHelper.longToBytes(1);
            bTree.put(bytes, bytes);
            bTree.commit();
            assertArrayEquals(null, bTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldBeABleToCreateNewDBWithLogFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final byte[] nonExistingKeyValuePair = BinaryHelper.longToBytes(1919191919L);
        final byte[] keyOne = BinaryHelper.longToBytes(1);
        final byte[] keyFive = BinaryHelper.longToBytes(5);
        final byte[] keyTen = BinaryHelper.longToBytes(10);

        try (final BTree bTree = createNewPersistedLogBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(keyOne, keyOne);
            bTree.put(keyTen, keyTen);
            bTree.put(keyFive, keyFive);

            assertArrayEquals(null, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(tempDirectory))
        {
            assertArrayEquals(keyOne, readBTree.get(keyOne));
            assertArrayEquals(keyTen, readBTree.get(keyTen));
            assertArrayEquals(keyFive, readBTree.get(keyFive));

            assertArrayEquals(null, readBTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDBWithLog() throws Exception
    {
        final byte[] key = BinaryHelper.longToBytes(123L);
        final int maxVersions = 100;
        final byte[] nonExistingKey = BinaryHelper.longToBytes(964L);

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                originalBTree.put(key, value);
                originalBTree.commit();
            }

            assertArrayEquals(null, originalBTree.get(nonExistingKey));
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < maxVersions; i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                assertArrayEquals(value, loadedBTree.get(key, i));
            }

            assertArrayEquals(null, loadedBTree.get(nonExistingKey));
        }
    }

    @Test
    void shouldGetHistoricValuesByTimestampFromOpenDB() throws Exception
    {
        final byte[] key = BinaryHelper.longToBytes(123L);
        final int maxVersions = 100;

        final StubTimeSource timeSource = new StubTimeSource();

        try (final BTreeWithLog originalBTree = createNewPersistedLogBtree(tempDirectory, TestUtils.BYTE_ORDER, timeSource))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                originalBTree.put(key, value);
            }

            originalBTree.commit();
        }

        try (final BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < timeSource.getCurrentTimeWithoutIncrementing(); i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                assertArrayEquals(value, loadedBTree.get(key, i));
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                originalBTree.put(bytes, bytes);
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                assertArrayEquals(bytes, loadedBTree.get(bytes));
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                originalBTree.put(bytes, bytes);
                originalBTree.commit();
            }
            originalBTree.commit();
        }

        //open the persisted tree and continue inserting elements
        try (BTreeWithLog loadedBTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = numberOfPairs; i < numberOfPairs * 2; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                loadedBTree.put(bytes, bytes);
                loadedBTree.commit();
            }
        }

        //open the persisted tree and read all the elements
        try (BTreeWithLog loadedBTree2 = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairs * 2; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                assertArrayEquals(bytes, loadedBTree2.get(bytes));
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                originalTree.put(bytes, bytes);
                originalTree.commit();
            }
            originalTree.commit();
        }


        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = startOffset; i < endDeletionOffset; i++)
            {
                final byte[] key = BinaryHelper.longToBytes(i);
                loadedTree.remove(key);
                loadedTree.commit();
            }
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"983\"[label = \"{ log | { 444- |  445- |  446- |  447- |  448- |  449- }}| <794> |40|  <843> |80|  <945> |160|  <lastChild> |Ls \"];\n" +
                "\"983\":794 -> \"794\"\n" +
                "\"983\":843 -> \"843\"\n" +
                "\"983\":945 -> \"945\"\n" +
                "\"983\":lastChild -> \"976\"\n" +
                "\"794\"[label = \" <728> |260|  <729> |10|  <739> |526|  <750> |20|  <751> |280|  <761> |30|  <772> |290|  <lastChild> |Ls \"];\n" +
                "\"794\":728 -> \"728\"\n" +
                "\"794\":729 -> \"729\"\n" +
                "\"794\":739 -> \"739\"\n" +
                "\"794\":750 -> \"750\"\n" +
                "\"794\":751 -> \"751\"\n" +
                "\"794\":761 -> \"761\"\n" +
                "\"794\":772 -> \"772\"\n" +
                "\"794\":lastChild -> \"783\"\n" +
                "\"728\"[label = \" <0> |0|  <256> |256|  <512> |512|  <1> |1|  <257> |257|  <513> |513|  <2> |2|  <258> |258|  <514> |514|  <3> |3|  <259> |259|  <515> |515|  <4> |4| \"];\n" +
                "\"729\"[label = \" <260> |260|  <516> |516|  <5> |5|  <261> |261|  <517> |517|  <6> |6|  <262> |262|  <518> |518|  <7> |7|  <263> |263|  <519> |519|  <8> |8|  <264> |264|  <520> |520|  <9> |9|  <265> |265|  <521> |521| \"];\n" +
                "\"739\"[label = \" <10> |10|  <266> |266|  <522> |522|  <11> |11|  <267> |267|  <523> |523|  <12> |12|  <268> |268|  <524> |524|  <13> |13|  <269> |269|  <525> |525|  <14> |14|  <270> |270| \"];\n" +
                "\"750\"[label = \" <526> |526|  <15> |15|  <271> |271|  <527> |527|  <16> |16|  <272> |272|  <528> |528|  <17> |17|  <273> |273|  <529> |529|  <18> |18|  <274> |274|  <530> |530|  <19> |19|  <275> |275|  <531> |531| \"];\n" +
                "\"751\"[label = \" <20> |20|  <276> |276|  <532> |532|  <21> |21|  <277> |277|  <533> |533|  <22> |22|  <278> |278|  <534> |534|  <23> |23|  <279> |279|  <535> |535|  <24> |24| \"];\n" +
                "\"761\"[label = \" <280> |280|  <536> |536|  <25> |25|  <281> |281|  <537> |537|  <26> |26|  <282> |282|  <538> |538|  <27> |27|  <283> |283|  <539> |539|  <28> |28|  <284> |284|  <540> |540|  <29> |29|  <285> |285|  <541> |541| \"];\n" +
                "\"772\"[label = \" <30> |30|  <286> |286|  <542> |542|  <31> |31|  <287> |287|  <543> |543|  <32> |32|  <288> |288|  <544> |544|  <33> |33|  <289> |289|  <545> |545|  <34> |34| \"];\n" +
                "\"783\"[label = \" <290> |290|  <546> |546|  <35> |35|  <291> |291|  <547> |547|  <36> |36|  <292> |292|  <548> |548|  <37> |37|  <293> |293|  <549> |549|  <38> |38|  <294> |294|  <550> |550|  <39> |39|  <295> |295|  <551> |551| \"];\n" +
                "\"843\"[label = \"{ log | { 585-585 |  586-586 |  587-587 |  588-588 |  590-590 |  591-591 }}| <784> |300|  <795> |50|  <809> |310|  <820> |60|  <821> |320|  <832> |70|  <833> |330|  <lastChild> |Ls \"];\n" +
                "\"843\":784 -> \"784\"\n" +
                "\"843\":795 -> \"795\"\n" +
                "\"843\":809 -> \"809\"\n" +
                "\"843\":820 -> \"820\"\n" +
                "\"843\":821 -> \"821\"\n" +
                "\"843\":832 -> \"832\"\n" +
                "\"843\":833 -> \"833\"\n" +
                "\"843\":lastChild -> \"834\"\n" +
                "\"784\"[label = \" <40> |40|  <296> |296|  <552> |552|  <41> |41|  <297> |297|  <553> |553|  <42> |42|  <298> |298|  <554> |554|  <43> |43|  <299> |299|  <555> |555|  <44> |44| \"];\n" +
                "\"795\"[label = \" <300> |300|  <556> |556|  <45> |45|  <301> |301|  <557> |557|  <46> |46|  <302> |302|  <558> |558|  <47> |47|  <303> |303|  <559> |559|  <48> |48|  <304> |304|  <560> |560|  <49> |49|  <305> |305|  <561> |561| \"];\n" +
                "\"809\"[label = \" <50> |50|  <306> |306|  <562> |562|  <51> |51|  <307> |307|  <563> |563|  <52> |52|  <308> |308|  <564> |564|  <53> |53|  <309> |309|  <565> |565|  <54> |54| \"];\n" +
                "\"820\"[label = \" <310> |310|  <566> |566|  <55> |55|  <311> |311|  <567> |567|  <56> |56|  <312> |312|  <568> |568|  <57> |57|  <313> |313|  <569> |569|  <58> |58|  <314> |314|  <570> |570|  <59> |59|  <315> |315|  <571> |571| \"];\n" +
                "\"821\"[label = \" <60> |60|  <316> |316|  <572> |572|  <61> |61|  <317> |317|  <573> |573|  <62> |62|  <318> |318|  <574> |574|  <63> |63|  <319> |319|  <575> |575|  <64> |64| \"];\n" +
                "\"832\"[label = \" <320> |320|  <576> |576|  <65> |65|  <321> |321|  <577> |577|  <66> |66|  <322> |322|  <578> |578|  <67> |67|  <323> |323|  <579> |579|  <68> |68|  <324> |324|  <580> |580|  <69> |69|  <325> |325|  <581> |581| \"];\n" +
                "\"833\"[label = \" <70> |70|  <326> |326|  <582> |582|  <71> |71|  <327> |327|  <583> |583|  <72> |72|  <328> |328|  <584> |584|  <73> |73|  <329> |329|  <74> |74| \"];\n" +
                "\"834\"[label = \" <330> |330|  <75> |75|  <331> |331|  <76> |76|  <332> |332|  <77> |77|  <333> |333|  <589> |589|  <78> |78|  <334> |334|  <79> |79|  <335> |335| \"];\n" +
                "\"945\"[label = \"{ log | { 408- |  409- |  411- |  412- |  413- |  414- |  415- }}| <857> |340|  <858> |90|  <872> |100|  <887> |110|  <902> |120|  <903> |130|  <917> |140|  <931> |150|  <lastChild> |Ls \"];\n" +
                "\"945\":857 -> \"857\"\n" +
                "\"945\":858 -> \"858\"\n" +
                "\"945\":872 -> \"872\"\n" +
                "\"945\":887 -> \"887\"\n" +
                "\"945\":902 -> \"902\"\n" +
                "\"945\":903 -> \"903\"\n" +
                "\"945\":917 -> \"917\"\n" +
                "\"945\":931 -> \"931\"\n" +
                "\"945\":lastChild -> \"932\"\n" +
                "\"857\"[label = \" <80> |80|  <336> |336|  <592> |592|  <81> |81|  <337> |337|  <593> |593|  <82> |82|  <338> |338|  <594> |594|  <83> |83|  <339> |339|  <595> |595|  <84> |84| \"];\n" +
                "\"858\"[label = \" <340> |340|  <596> |596|  <85> |85|  <341> |341|  <597> |597|  <86> |86|  <342> |342|  <598> |598|  <87> |87|  <343> |343|  <599> |599|  <88> |88|  <344> |344|  <89> |89|  <345> |345| \"];\n" +
                "\"872\"[label = \" <90> |90|  <346> |346|  <91> |91|  <347> |347|  <92> |92|  <348> |348|  <93> |93|  <349> |349|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                "\"887\"[label = \" <100> |100|  <101> |101|  <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107|  <108> |108|  <109> |109| \"];\n" +
                "\"902\"[label = \" <110> |110|  <111> |111|  <112> |112|  <113> |113|  <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"903\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125|  <126> |126|  <127> |127|  <128> |128|  <129> |129| \"];\n" +
                "\"917\"[label = \" <130> |130|  <131> |131|  <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137|  <138> |138|  <139> |139| \"];\n" +
                "\"931\"[label = \" <140> |140|  <141> |141|  <142> |142|  <143> |143|  <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"932\"[label = \" <150> |150|  <151> |151|  <152> |152|  <408> |408|  <153> |153|  <409> |409|  <154> |154|  <155> |155|  <411> |411|  <156> |156|  <412> |412|  <157> |157|  <413> |413|  <158> |158|  <414> |414|  <159> |159|  <415> |415| \"];\n" +
                "\"976\"[label = \"{ log | { 435- |  436- |  437- |  438- |  439- |  440- |  441- |  442- }}| <973> |170|  <974> |180|  <975> |190|  <637> |200|  <647> |210|  <667> |220|  <677> |230|  <687> |240|  <697> |246|  <lastChild> |Ls \"];\n" +
                "\"976\":973 -> \"973\"\n" +
                "\"976\":974 -> \"974\"\n" +
                "\"976\":975 -> \"975\"\n" +
                "\"976\":637 -> \"637\"\n" +
                "\"976\":647 -> \"647\"\n" +
                "\"976\":667 -> \"667\"\n" +
                "\"976\":677 -> \"677\"\n" +
                "\"976\":687 -> \"687\"\n" +
                "\"976\":697 -> \"697\"\n" +
                "\"976\":lastChild -> \"960\"\n" +
                "\"973\"[label = \" <160> |160|  <161> |161|  <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167|  <168> |168|  <169> |169| \"];\n" +
                "\"974\"[label = \" <170> |170|  <171> |171|  <172> |172|  <173> |173|  <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179|  <435> |435| \"];\n" +
                "\"975\"[label = \" <180> |180|  <436> |436|  <181> |181|  <437> |437|  <182> |182|  <438> |438|  <183> |183|  <439> |439|  <184> |184|  <440> |440|  <185> |185|  <441> |441|  <186> |186|  <442> |442|  <187> |187|  <188> |188|  <444> |444|  <189> |189|  <445> |445| \"];\n" +
                "\"637\"[label = \" <190> |190|  <446> |446|  <191> |191|  <447> |447|  <192> |192|  <448> |448|  <193> |193|  <449> |449|  <194> |194|  <450> |450|  <195> |195|  <451> |451|  <196> |196|  <452> |452|  <197> |197|  <453> |453|  <198> |198|  <454> |454|  <199> |199|  <455> |455| \"];\n" +
                "\"647\"[label = \" <200> |200|  <456> |456|  <201> |201|  <457> |457|  <202> |202|  <458> |458|  <203> |203|  <459> |459|  <204> |204|  <460> |460|  <205> |205|  <461> |461|  <206> |206|  <462> |462|  <207> |207|  <463> |463|  <208> |208|  <464> |464|  <209> |209|  <465> |465| \"];\n" +
                "\"667\"[label = \" <210> |210|  <466> |466|  <211> |211|  <467> |467|  <212> |212|  <468> |468|  <213> |213|  <469> |469|  <214> |214|  <470> |470|  <215> |215|  <471> |471|  <216> |216|  <472> |472|  <217> |217|  <473> |473|  <218> |218|  <474> |474|  <219> |219|  <475> |475| \"];\n" +
                "\"677\"[label = \" <220> |220|  <476> |476|  <221> |221|  <477> |477|  <222> |222|  <478> |478|  <223> |223|  <479> |479|  <224> |224|  <480> |480|  <225> |225|  <481> |481|  <226> |226|  <482> |482|  <227> |227|  <483> |483|  <228> |228|  <484> |484|  <229> |229|  <485> |485| \"];\n" +
                "\"687\"[label = \" <230> |230|  <486> |486|  <231> |231|  <487> |487|  <232> |232|  <488> |488|  <233> |233|  <489> |489|  <234> |234|  <490> |490|  <235> |235|  <491> |491|  <236> |236|  <492> |492|  <237> |237|  <493> |493|  <238> |238|  <494> |494|  <239> |239|  <495> |495| \"];\n" +
                "\"697\"[label = \" <240> |240|  <496> |496|  <241> |241|  <497> |497|  <242> |242|  <498> |498|  <243> |243|  <499> |499|  <244> |244|  <500> |500|  <245> |245|  <501> |501| \"];\n" +
                "\"960\"[label = \" <246> |246|  <502> |502|  <247> |247|  <503> |503|  <248> |248|  <504> |504|  <249> |249|  <505> |505|  <250> |250|  <506> |506|  <251> |251|  <507> |507|  <252> |252|  <508> |508|  <253> |253|  <509> |509|  <254> |254|  <510> |510|  <255> |255|  <511> |511| \"];\n" +
                "}\n";

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairsToInsert; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                //delete range,don't assert on it
                if (i >= startOffset && i < endDeletionOffset)
                {
                    assertArrayEquals(null, loadedTree.get(bytes));
                }
                else
                {
                    assertArrayEquals(bytes, loadedTree.get(bytes));
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                newTree.put(bytes, bytes);
                newTree.commit();
            }
            newTree.commit();
        }

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (long i = startOffset; i < endDeletionOffset; i++)
            {
                final byte[] key = BinaryHelper.longToBytes(i);
                loadedTree.remove(key);
                loadedTree.commit();
            }
        }

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = startOffset; i < endDeletionOffset; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                loadedTree.put(bytes, bytes);
                loadedTree.commit();
            }
        }

        try (BTreeWithLog loadedTree = loadPersistedLogBtree(tempDirectory))
        {
            for (int i = 0; i < numberOfPairsToInsert; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                assertArrayEquals(bytes, loadedTree.get(bytes));
            }
        }
    }
}
