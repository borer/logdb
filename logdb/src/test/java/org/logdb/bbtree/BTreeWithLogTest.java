package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.bit.BinaryHelper;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;
import static org.logdb.support.TestUtils.createInitialRootReference;
import static org.logdb.support.TestUtils.createRootIndex;

class BTreeWithLogTest
{
    private static final int PAGE_SIZE = 256;
    private static final int MAX_LOG_SIZE = 76;
    private static final int EXPECTED_NODES = 65;
    private BTreeWithLog bTree;

    @BeforeEach
    void setUp()
    {
        final Storage treeStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE, MEMORY_CHUNK_SIZE);
        final RootIndex rootIndex = createRootIndex(PAGE_SIZE);

        NodesManager nodesManager = new NodesManager(treeStorage, rootIndex, true, MAX_LOG_SIZE);
        bTree = new BTreeWithLog(
                nodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager)
        );
    }

    @Test
    void shouldBeAbleToRetrieveNonExistingElementsWithLog()
    {
        for (long i = 0; i < 10; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(null, bTree.get(bytes));
        }
    }

    @Test
    void shouldBeAbleToRetrievePastVersionsForElementsWithLog()
    {
        final long key = 123L;
        final byte[] keyBytes = BinaryHelper.longToBytes(key);
        for (long i = 0; i < 200; i++)
        {
            final byte[] value = BinaryHelper.longToBytes(i);
            bTree.put(keyBytes, value);
        }

        for (long i = 0; i < 200; i++)
        {
            final byte[] value = BinaryHelper.longToBytes(i);
            assertArrayEquals(value, bTree.get(keyBytes, (int)i));
        }
    }

    @Test
    void shouldBeAbleToPutElementsWithLog()
    {
        for (long i = 0; i < 600; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"1151\"[label = \" <1037> |32|  <1103> |64|  <1152> |96|  <1157> |128|  <832> |192|  <lastChild> |Ls \"];\n" +
                "\"1151\":1037 -> \"1037\"\n" +
                "\"1151\":1103 -> \"1103\"\n" +
                "\"1151\":1152 -> \"1152\"\n" +
                "\"1151\":1157 -> \"1157\"\n" +
                "\"1151\":832 -> \"832\"\n" +
                "\"1151\":lastChild -> \"970\"\n" +
                "\"1037\"[label = \" <987> |8|  <1004> |16|  <1020> |24|  <lastChild> |Ls \"];\n" +
                "\"1037\":987 -> \"987\"\n" +
                "\"1037\":1004 -> \"1004\"\n" +
                "\"1037\":1020 -> \"1020\"\n" +
                "\"1037\":lastChild -> \"1038\"\n" +
                "\"987\"[label = \" <980> |2|  <981> |4|  <988> |6|  <lastChild> |Ls \"];\n" +
                "\"987\":980 -> \"980\"\n" +
                "\"987\":981 -> \"981\"\n" +
                "\"987\":988 -> \"988\"\n" +
                "\"987\":lastChild -> \"989\"\n" +
                "\"980\"[label = \" <0> |0|  <256> |256|  <512> |512|  <1> |1|  <257> |257|  <513> |513| \"];\n" +
                "\"981\"[label = \" <2> |2|  <258> |258|  <514> |514|  <3> |3|  <259> |259|  <515> |515| \"];\n" +
                "\"988\"[label = \" <4> |4|  <260> |260|  <516> |516|  <5> |5|  <261> |261|  <517> |517| \"];\n" +
                "\"989\"[label = \" <6> |6|  <262> |262|  <518> |518|  <7> |7|  <263> |263|  <519> |519| \"];\n" +
                "\"1004\"[label = \" <997> |10|  <998> |12|  <1005> |14|  <lastChild> |Ls \"];\n" +
                "\"1004\":997 -> \"997\"\n" +
                "\"1004\":998 -> \"998\"\n" +
                "\"1004\":1005 -> \"1005\"\n" +
                "\"1004\":lastChild -> \"1006\"\n" +
                "\"997\"[label = \" <8> |8|  <264> |264|  <520> |520|  <9> |9|  <265> |265|  <521> |521| \"];\n" +
                "\"998\"[label = \" <10> |10|  <266> |266|  <522> |522|  <11> |11|  <267> |267|  <523> |523| \"];\n" +
                "\"1005\"[label = \" <12> |12|  <268> |268|  <524> |524|  <13> |13|  <269> |269|  <525> |525| \"];\n" +
                "\"1006\"[label = \" <14> |14|  <270> |270|  <526> |526|  <15> |15|  <271> |271|  <527> |527| \"];\n" +
                "\"1020\"[label = \" <1013> |18|  <1014> |20|  <1021> |22|  <lastChild> |Ls \"];\n" +
                "\"1020\":1013 -> \"1013\"\n" +
                "\"1020\":1014 -> \"1014\"\n" +
                "\"1020\":1021 -> \"1021\"\n" +
                "\"1020\":lastChild -> \"1022\"\n" +
                "\"1013\"[label = \" <16> |16|  <272> |272|  <528> |528|  <17> |17|  <273> |273|  <529> |529| \"];\n" +
                "\"1014\"[label = \" <18> |18|  <274> |274|  <530> |530|  <19> |19|  <275> |275|  <531> |531| \"];\n" +
                "\"1021\"[label = \" <20> |20|  <276> |276|  <532> |532|  <21> |21|  <277> |277|  <533> |533| \"];\n" +
                "\"1022\"[label = \" <22> |22|  <278> |278|  <534> |534|  <23> |23|  <279> |279|  <535> |535| \"];\n" +
                "\"1038\"[label = \" <1031> |26|  <1032> |28|  <1039> |30|  <lastChild> |Ls \"];\n" +
                "\"1038\":1031 -> \"1031\"\n" +
                "\"1038\":1032 -> \"1032\"\n" +
                "\"1038\":1039 -> \"1039\"\n" +
                "\"1038\":lastChild -> \"1040\"\n" +
                "\"1031\"[label = \" <24> |24|  <280> |280|  <536> |536|  <25> |25|  <281> |281|  <537> |537| \"];\n" +
                "\"1032\"[label = \" <26> |26|  <282> |282|  <538> |538|  <27> |27|  <283> |283|  <539> |539| \"];\n" +
                "\"1039\"[label = \" <28> |28|  <284> |284|  <540> |540|  <29> |29|  <285> |285|  <541> |541| \"];\n" +
                "\"1040\"[label = \" <30> |30|  <286> |286|  <542> |542|  <31> |31|  <287> |287|  <543> |543| \"];\n" +
                "\"1103\"[label = \" <1054> |40|  <1071> |48|  <1087> |56|  <lastChild> |Ls \"];\n" +
                "\"1103\":1054 -> \"1054\"\n" +
                "\"1103\":1071 -> \"1071\"\n" +
                "\"1103\":1087 -> \"1087\"\n" +
                "\"1103\":lastChild -> \"1104\"\n" +
                "\"1054\"[label = \" <1047> |34|  <1048> |36|  <1055> |38|  <lastChild> |Ls \"];\n" +
                "\"1054\":1047 -> \"1047\"\n" +
                "\"1054\":1048 -> \"1048\"\n" +
                "\"1054\":1055 -> \"1055\"\n" +
                "\"1054\":lastChild -> \"1056\"\n" +
                "\"1047\"[label = \" <32> |32|  <288> |288|  <544> |544|  <33> |33|  <289> |289|  <545> |545| \"];\n" +
                "\"1048\"[label = \" <34> |34|  <290> |290|  <546> |546|  <35> |35|  <291> |291|  <547> |547| \"];\n" +
                "\"1055\"[label = \" <36> |36|  <292> |292|  <548> |548|  <37> |37|  <293> |293|  <549> |549| \"];\n" +
                "\"1056\"[label = \" <38> |38|  <294> |294|  <550> |550|  <39> |39|  <295> |295|  <551> |551| \"];\n" +
                "\"1071\"[label = \" <1064> |42|  <1065> |44|  <1072> |46|  <lastChild> |Ls \"];\n" +
                "\"1071\":1064 -> \"1064\"\n" +
                "\"1071\":1065 -> \"1065\"\n" +
                "\"1071\":1072 -> \"1072\"\n" +
                "\"1071\":lastChild -> \"1073\"\n" +
                "\"1064\"[label = \" <40> |40|  <296> |296|  <552> |552|  <41> |41|  <297> |297|  <553> |553| \"];\n" +
                "\"1065\"[label = \" <42> |42|  <298> |298|  <554> |554|  <43> |43|  <299> |299|  <555> |555| \"];\n" +
                "\"1072\"[label = \" <44> |44|  <300> |300|  <556> |556|  <45> |45|  <301> |301|  <557> |557| \"];\n" +
                "\"1073\"[label = \" <46> |46|  <302> |302|  <558> |558|  <47> |47|  <303> |303|  <559> |559| \"];\n" +
                "\"1087\"[label = \" <1080> |50|  <1081> |52|  <1088> |54|  <lastChild> |Ls \"];\n" +
                "\"1087\":1080 -> \"1080\"\n" +
                "\"1087\":1081 -> \"1081\"\n" +
                "\"1087\":1088 -> \"1088\"\n" +
                "\"1087\":lastChild -> \"1089\"\n" +
                "\"1080\"[label = \" <48> |48|  <304> |304|  <560> |560|  <49> |49|  <305> |305|  <561> |561| \"];\n" +
                "\"1081\"[label = \" <50> |50|  <306> |306|  <562> |562|  <51> |51|  <307> |307|  <563> |563| \"];\n" +
                "\"1088\"[label = \" <52> |52|  <308> |308|  <564> |564|  <53> |53|  <309> |309|  <565> |565| \"];\n" +
                "\"1089\"[label = \" <54> |54|  <310> |310|  <566> |566|  <55> |55|  <311> |311|  <567> |567| \"];\n" +
                "\"1104\"[label = \" <1097> |58|  <1098> |60|  <1105> |62|  <lastChild> |Ls \"];\n" +
                "\"1104\":1097 -> \"1097\"\n" +
                "\"1104\":1098 -> \"1098\"\n" +
                "\"1104\":1105 -> \"1105\"\n" +
                "\"1104\":lastChild -> \"1106\"\n" +
                "\"1097\"[label = \" <56> |56|  <312> |312|  <568> |568|  <57> |57|  <313> |313|  <569> |569| \"];\n" +
                "\"1098\"[label = \" <58> |58|  <314> |314|  <570> |570|  <59> |59|  <315> |315|  <571> |571| \"];\n" +
                "\"1105\"[label = \" <60> |60|  <316> |316|  <572> |572|  <61> |61|  <317> |317|  <573> |573| \"];\n" +
                "\"1106\"[label = \" <62> |62|  <318> |318|  <574> |574|  <63> |63|  <319> |319|  <575> |575| \"];\n" +
                "\"1152\"[label = \" <1120> |72|  <1137> |80|  <1153> |88|  <lastChild> |Ls \"];\n" +
                "\"1152\":1120 -> \"1120\"\n" +
                "\"1152\":1137 -> \"1137\"\n" +
                "\"1152\":1153 -> \"1153\"\n" +
                "\"1152\":lastChild -> \"1156\"\n" +
                "\"1120\"[label = \" <1113> |66|  <1114> |68|  <1121> |70|  <lastChild> |Ls \"];\n" +
                "\"1120\":1113 -> \"1113\"\n" +
                "\"1120\":1114 -> \"1114\"\n" +
                "\"1120\":1121 -> \"1121\"\n" +
                "\"1120\":lastChild -> \"1122\"\n" +
                "\"1113\"[label = \" <64> |64|  <320> |320|  <576> |576|  <65> |65|  <321> |321|  <577> |577| \"];\n" +
                "\"1114\"[label = \" <66> |66|  <322> |322|  <578> |578|  <67> |67|  <323> |323|  <579> |579| \"];\n" +
                "\"1121\"[label = \" <68> |68|  <324> |324|  <580> |580|  <69> |69|  <325> |325|  <581> |581| \"];\n" +
                "\"1122\"[label = \" <70> |70|  <326> |326|  <582> |582|  <71> |71|  <327> |327|  <583> |583| \"];\n" +
                "\"1137\"[label = \" <1130> |74|  <1131> |76|  <1138> |78|  <lastChild> |Ls \"];\n" +
                "\"1137\":1130 -> \"1130\"\n" +
                "\"1137\":1131 -> \"1131\"\n" +
                "\"1137\":1138 -> \"1138\"\n" +
                "\"1137\":lastChild -> \"1139\"\n" +
                "\"1130\"[label = \" <72> |72|  <328> |328|  <584> |584|  <73> |73|  <329> |329|  <585> |585| \"];\n" +
                "\"1131\"[label = \" <74> |74|  <330> |330|  <586> |586|  <75> |75|  <331> |331|  <587> |587| \"];\n" +
                "\"1138\"[label = \" <76> |76|  <332> |332|  <588> |588|  <77> |77|  <333> |333|  <589> |589| \"];\n" +
                "\"1139\"[label = \" <78> |78|  <334> |334|  <590> |590|  <79> |79|  <335> |335|  <591> |591| \"];\n" +
                "\"1153\"[label = \" <1146> |82|  <1147> |84|  <1154> |86|  <lastChild> |Ls \"];\n" +
                "\"1153\":1146 -> \"1146\"\n" +
                "\"1153\":1147 -> \"1147\"\n" +
                "\"1153\":1154 -> \"1154\"\n" +
                "\"1153\":lastChild -> \"1155\"\n" +
                "\"1146\"[label = \" <80> |80|  <336> |336|  <592> |592|  <81> |81|  <337> |337|  <593> |593| \"];\n" +
                "\"1147\"[label = \" <82> |82|  <338> |338|  <594> |594|  <83> |83|  <339> |339|  <595> |595| \"];\n" +
                "\"1154\"[label = \" <84> |84|  <340> |340|  <596> |596|  <85> |85|  <341> |341|  <597> |597| \"];\n" +
                "\"1155\"[label = \" <86> |86|  <342> |342|  <598> |598|  <87> |87|  <343> |343|  <599> |599| \"];\n" +
                "\"1156\"[label = \" <659> |92|  <lastChild> |Ls \"];\n" +
                "\"1156\":659 -> \"659\"\n" +
                "\"1156\":lastChild -> \"666\"\n" +
                "\"659\"[label = \" <88> |88|  <344> |344|  <89> |89|  <345> |345|  <90> |90|  <346> |346|  <91> |91|  <347> |347| \"];\n" +
                "\"666\"[label = \" <92> |92|  <348> |348|  <93> |93|  <349> |349|  <94> |94|  <350> |350|  <95> |95|  <351> |351| \"];\n" +
                "\"1157\"[label = \" <693> |112|  <lastChild> |Ls \"];\n" +
                "\"1157\":693 -> \"693\"\n" +
                "\"1157\":lastChild -> \"721\"\n" +
                "\"693\"[label = \" <673> |100|  <680> |104|  <687> |108|  <lastChild> |Ls \"];\n" +
                "\"693\":673 -> \"673\"\n" +
                "\"693\":680 -> \"680\"\n" +
                "\"693\":687 -> \"687\"\n" +
                "\"693\":lastChild -> \"694\"\n" +
                "\"673\"[label = \" <96> |96|  <352> |352|  <97> |97|  <353> |353|  <98> |98|  <354> |354|  <99> |99|  <355> |355| \"];\n" +
                "\"680\"[label = \" <100> |100|  <356> |356|  <101> |101|  <357> |357|  <102> |102|  <358> |358|  <103> |103|  <359> |359| \"];\n" +
                "\"687\"[label = \" <104> |104|  <360> |360|  <105> |105|  <361> |361|  <106> |106|  <362> |362|  <107> |107|  <363> |363| \"];\n" +
                "\"694\"[label = \" <108> |108|  <364> |364|  <109> |109|  <365> |365|  <110> |110|  <366> |366|  <111> |111|  <367> |367| \"];\n" +
                "\"721\"[label = \" <701> |116|  <708> |120|  <715> |124|  <lastChild> |Ls \"];\n" +
                "\"721\":701 -> \"701\"\n" +
                "\"721\":708 -> \"708\"\n" +
                "\"721\":715 -> \"715\"\n" +
                "\"721\":lastChild -> \"722\"\n" +
                "\"701\"[label = \" <112> |112|  <368> |368|  <113> |113|  <369> |369|  <114> |114|  <370> |370|  <115> |115|  <371> |371| \"];\n" +
                "\"708\"[label = \" <116> |116|  <372> |372|  <117> |117|  <373> |373|  <118> |118|  <374> |374|  <119> |119|  <375> |375| \"];\n" +
                "\"715\"[label = \" <120> |120|  <376> |376|  <121> |121|  <377> |377|  <122> |122|  <378> |378|  <123> |123|  <379> |379| \"];\n" +
                "\"722\"[label = \" <124> |124|  <380> |380|  <125> |125|  <381> |381|  <126> |126|  <382> |382|  <127> |127|  <383> |383| \"];\n" +
                "\"832\"[label = \" <749> |144|  <777> |160|  <805> |176|  <lastChild> |Ls \"];\n" +
                "\"832\":749 -> \"749\"\n" +
                "\"832\":777 -> \"777\"\n" +
                "\"832\":805 -> \"805\"\n" +
                "\"832\":lastChild -> \"833\"\n" +
                "\"749\"[label = \" <729> |132|  <736> |136|  <743> |140|  <lastChild> |Ls \"];\n" +
                "\"749\":729 -> \"729\"\n" +
                "\"749\":736 -> \"736\"\n" +
                "\"749\":743 -> \"743\"\n" +
                "\"749\":lastChild -> \"750\"\n" +
                "\"729\"[label = \" <128> |128|  <384> |384|  <129> |129|  <385> |385|  <130> |130|  <386> |386|  <131> |131|  <387> |387| \"];\n" +
                "\"736\"[label = \" <132> |132|  <388> |388|  <133> |133|  <389> |389|  <134> |134|  <390> |390|  <135> |135|  <391> |391| \"];\n" +
                "\"743\"[label = \" <136> |136|  <392> |392|  <137> |137|  <393> |393|  <138> |138|  <394> |394|  <139> |139|  <395> |395| \"];\n" +
                "\"750\"[label = \" <140> |140|  <396> |396|  <141> |141|  <397> |397|  <142> |142|  <398> |398|  <143> |143|  <399> |399| \"];\n" +
                "\"777\"[label = \" <757> |148|  <764> |152|  <771> |156|  <lastChild> |Ls \"];\n" +
                "\"777\":757 -> \"757\"\n" +
                "\"777\":764 -> \"764\"\n" +
                "\"777\":771 -> \"771\"\n" +
                "\"777\":lastChild -> \"778\"\n" +
                "\"757\"[label = \" <144> |144|  <400> |400|  <145> |145|  <401> |401|  <146> |146|  <402> |402|  <147> |147|  <403> |403| \"];\n" +
                "\"764\"[label = \" <148> |148|  <404> |404|  <149> |149|  <405> |405|  <150> |150|  <406> |406|  <151> |151|  <407> |407| \"];\n" +
                "\"771\"[label = \" <152> |152|  <408> |408|  <153> |153|  <409> |409|  <154> |154|  <410> |410|  <155> |155|  <411> |411| \"];\n" +
                "\"778\"[label = \" <156> |156|  <412> |412|  <157> |157|  <413> |413|  <158> |158|  <414> |414|  <159> |159|  <415> |415| \"];\n" +
                "\"805\"[label = \" <785> |164|  <792> |168|  <799> |172|  <lastChild> |Ls \"];\n" +
                "\"805\":785 -> \"785\"\n" +
                "\"805\":792 -> \"792\"\n" +
                "\"805\":799 -> \"799\"\n" +
                "\"805\":lastChild -> \"806\"\n" +
                "\"785\"[label = \" <160> |160|  <416> |416|  <161> |161|  <417> |417|  <162> |162|  <418> |418|  <163> |163|  <419> |419| \"];\n" +
                "\"792\"[label = \" <164> |164|  <420> |420|  <165> |165|  <421> |421|  <166> |166|  <422> |422|  <167> |167|  <423> |423| \"];\n" +
                "\"799\"[label = \" <168> |168|  <424> |424|  <169> |169|  <425> |425|  <170> |170|  <426> |426|  <171> |171|  <427> |427| \"];\n" +
                "\"806\"[label = \" <172> |172|  <428> |428|  <173> |173|  <429> |429|  <174> |174|  <430> |430|  <175> |175|  <431> |431| \"];\n" +
                "\"833\"[label = \" <813> |180|  <820> |184|  <827> |188|  <lastChild> |Ls \"];\n" +
                "\"833\":813 -> \"813\"\n" +
                "\"833\":820 -> \"820\"\n" +
                "\"833\":827 -> \"827\"\n" +
                "\"833\":lastChild -> \"834\"\n" +
                "\"813\"[label = \" <176> |176|  <432> |432|  <177> |177|  <433> |433|  <178> |178|  <434> |434|  <179> |179|  <435> |435| \"];\n" +
                "\"820\"[label = \" <180> |180|  <436> |436|  <181> |181|  <437> |437|  <182> |182|  <438> |438|  <183> |183|  <439> |439| \"];\n" +
                "\"827\"[label = \" <184> |184|  <440> |440|  <185> |185|  <441> |441|  <186> |186|  <442> |442|  <187> |187|  <443> |443| \"];\n" +
                "\"834\"[label = \" <188> |188|  <444> |444|  <189> |189|  <445> |445|  <190> |190|  <446> |446|  <191> |191|  <447> |447| \"];\n" +
                "\"970\"[label = \"{ log | { 510-510 }}| <874> |208|  <909> |224|  <944> |240|  <lastChild> |Ls \"];\n" +
                "\"970\":874 -> \"874\"\n" +
                "\"970\":909 -> \"909\"\n" +
                "\"970\":944 -> \"944\"\n" +
                "\"970\":lastChild -> \"971\"\n" +
                "\"874\"[label = \" <857> |196|  <866> |200|  <876> |204|  <lastChild> |Ls \"];\n" +
                "\"874\":857 -> \"857\"\n" +
                "\"874\":866 -> \"866\"\n" +
                "\"874\":876 -> \"876\"\n" +
                "\"874\":lastChild -> \"875\"\n" +
                "\"857\"[label = \" <192> |192|  <448> |448|  <193> |193|  <449> |449|  <194> |194|  <450> |450|  <195> |195|  <451> |451| \"];\n" +
                "\"866\"[label = \" <196> |196|  <452> |452|  <197> |197|  <453> |453|  <198> |198|  <454> |454|  <199> |199|  <455> |455| \"];\n" +
                "\"876\"[label = \" <200> |200|  <456> |456|  <201> |201|  <457> |457|  <202> |202|  <458> |458|  <203> |203|  <459> |459| \"];\n" +
                "\"875\"[label = \" <204> |204|  <460> |460|  <205> |205|  <461> |461|  <206> |206|  <462> |462|  <207> |207|  <463> |463| \"];\n" +
                "\"909\"[label = \" <892> |212|  <901> |216|  <911> |220|  <lastChild> |Ls \"];\n" +
                "\"909\":892 -> \"892\"\n" +
                "\"909\":901 -> \"901\"\n" +
                "\"909\":911 -> \"911\"\n" +
                "\"909\":lastChild -> \"910\"\n" +
                "\"892\"[label = \" <208> |208|  <464> |464|  <209> |209|  <465> |465|  <210> |210|  <466> |466|  <211> |211|  <467> |467| \"];\n" +
                "\"901\"[label = \" <212> |212|  <468> |468|  <213> |213|  <469> |469|  <214> |214|  <470> |470|  <215> |215|  <471> |471| \"];\n" +
                "\"911\"[label = \" <216> |216|  <472> |472|  <217> |217|  <473> |473|  <218> |218|  <474> |474|  <219> |219|  <475> |475| \"];\n" +
                "\"910\"[label = \" <220> |220|  <476> |476|  <221> |221|  <477> |477|  <222> |222|  <478> |478|  <223> |223|  <479> |479| \"];\n" +
                "\"944\"[label = \" <927> |228|  <936> |232|  <946> |236|  <lastChild> |Ls \"];\n" +
                "\"944\":927 -> \"927\"\n" +
                "\"944\":936 -> \"936\"\n" +
                "\"944\":946 -> \"946\"\n" +
                "\"944\":lastChild -> \"945\"\n" +
                "\"927\"[label = \" <224> |224|  <480> |480|  <225> |225|  <481> |481|  <226> |226|  <482> |482|  <227> |227|  <483> |483| \"];\n" +
                "\"936\"[label = \" <228> |228|  <484> |484|  <229> |229|  <485> |485|  <230> |230|  <486> |486|  <231> |231|  <487> |487| \"];\n" +
                "\"946\"[label = \" <232> |232|  <488> |488|  <233> |233|  <489> |489|  <234> |234|  <490> |490|  <235> |235|  <491> |491| \"];\n" +
                "\"945\"[label = \" <236> |236|  <492> |492|  <237> |237|  <493> |493|  <238> |238|  <494> |494|  <239> |239|  <495> |495| \"];\n" +
                "\"971\"[label = \"{ log | { 511-511 }}| <956> |244|  <964> |248|  <973> |251|  <lastChild> |Ls \"];\n" +
                "\"971\":956 -> \"956\"\n" +
                "\"971\":964 -> \"964\"\n" +
                "\"971\":973 -> \"973\"\n" +
                "\"971\":lastChild -> \"972\"\n" +
                "\"956\"[label = \" <240> |240|  <496> |496|  <241> |241|  <497> |497|  <242> |242|  <498> |498|  <243> |243|  <499> |499| \"];\n" +
                "\"964\"[label = \" <244> |244|  <500> |500|  <245> |245|  <501> |501|  <246> |246|  <502> |502|  <247> |247|  <503> |503| \"];\n" +
                "\"973\"[label = \" <248> |248|  <504> |504|  <249> |249|  <505> |505|  <250> |250|  <506> |506| \"];\n" +
                "\"972\"[label = \" <251> |251|  <507> |507|  <252> |252|  <508> |508|  <253> |253|  <509> |509|  <254> |254|  <255> |255| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }

    @Test
    void shouldBeAbleToRetrieveElementsWithLog()
    {
        final int size = 500;
        for (long i = 0; i < size; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        for (long i = 0; i < size; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
             assertArrayEquals(bytes, bTree.get(bytes));
        }
    }

    @Test
    void shouldBeAbleToGetElementsFromPast()
    {
        final byte[] key = BinaryHelper.longToBytes(5L);
        final byte[] expectedValue1 = BinaryHelper.longToBytes(1L);
        final byte[] expectedValue2 = BinaryHelper.longToBytes(2L);
        final byte[] expectedValue3 = BinaryHelper.longToBytes(3L);

        bTree.put(key, expectedValue1);
        bTree.put(key, expectedValue2);
        bTree.put(key, expectedValue3);

        //index for history is 0 based
        assertArrayEquals(expectedValue1, bTree.get(key, 0));
        assertArrayEquals(expectedValue2, bTree.get(key, 1));
        assertArrayEquals(expectedValue3, bTree.get(key, 2));

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
        final byte[] actualLatest = bTree.get(key);
        assertArrayEquals(expectedValue3, actualLatest);
    }

    @Test
    void shouldNotFailToDeleteNonExistingKeyWithLogWithoutFalsePositives()
    {
        assertEquals(1, bTree.getNodesCount());

        final int numberOfPairs = 200;
        for (int i = 0; i < numberOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            bTree.removeWithoutFalsePositives(key);
        }

        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        final byte[] key200 = BinaryHelper.longToBytes(200);
        final byte[] keyMinus20 = BinaryHelper.longToBytes(-20);
        bTree.removeWithoutFalsePositives(key200);
        bTree.removeWithoutFalsePositives(keyMinus20);

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        assertArrayEquals(null, bTree.get(key200));
        assertArrayEquals(null, bTree.get(keyMinus20));

        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(bytes, bTree.get(bytes));
        }
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWithoutFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        for (int i = 0; i < numberOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            bTree.removeWithoutFalsePositives(key);

            //verify that we still have the rest of the key/values
            if (i + 1 < numberOfPairs)
            {
                for (int j = i + 1; j < numberOfPairs; j++)
                {
                    final byte[] bytes = BinaryHelper.longToBytes(j);
                    assertArrayEquals(bytes, bTree.get(bytes));
                }
            }
        }

        assertEquals(1L, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"1004\"[label = \"\"];\n" +
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
            final byte[] key = BinaryHelper.longToBytes(i);
            bTree.remove(key);
        }

        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        final byte[] key200 = BinaryHelper.longToBytes(200);
        final byte[] keyMinus20 = BinaryHelper.longToBytes(-20);
        bTree.remove(key200);
        bTree.remove(keyMinus20);

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        assertNull(bTree.get(key200));
        assertNull(bTree.get(keyMinus20));

        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            assertArrayEquals(bytes, bTree.get(bytes));
        }
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWithFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            final byte[] bytes = BinaryHelper.longToBytes(i);
            bTree.put(bytes, bytes);
        }

        for (int i = 0; i < numberOfPairs; i++)
        {
            final byte[] key = BinaryHelper.longToBytes(i);
            bTree.remove(key);
        }

        assertEquals(1, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"735\"[label = \"\"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}