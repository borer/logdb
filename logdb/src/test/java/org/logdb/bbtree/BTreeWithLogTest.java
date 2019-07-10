package org.logdb.bbtree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.memory.MemoryStorage;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;
import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.MEMORY_CHUNK_SIZE;
import static org.logdb.support.TestUtils.createInitialRootReference;
import static org.logdb.support.TestUtils.createRootIndex;

class BTreeWithLogTest
{
    private static final int PAGE_SIZE = 256;
    private BTreeWithLog bTree;

    @BeforeEach
    void setUp()
    {
        final Storage treeStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE, MEMORY_CHUNK_SIZE);
        final RootIndex rootIndex = createRootIndex(PAGE_SIZE);

        NodesManager nodesManager = new NodesManager(treeStorage, rootIndex);
        bTree = new BTreeWithLog(
                nodesManager,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager));
    }

    @Test
    void shouldBeAbleToRetrieveNonExistingElementsWithLog()
    {
        for (long i = 0; i < 10; i++)
        {
            assertEquals(InvalidBTreeValues.KEY_NOT_FOUND_VALUE, bTree.get(i));
        }
    }

    @Test
    void shouldBeAbleToRetrievePastVersionsForElementsWithLog()
    {
        final long key = 123L;
        for (long i = 0; i < 200; i++)
        {
            bTree.put(key, i);
        }

        for (long i = 0; i < 200; i++)
        {
            assertEquals(i, bTree.get(key, (int)i));
        }
    }

    @Test
    void shouldBeAbleToPutElementsWithLog()
    {
        for (long i = 0; i < 500; i++)
        {
            bTree.put(i, i);
        }

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"751\"[label = \" <658> |216|  <lastChild> |Ls \"];\n" +
                "\"751\":658 -> \"658\"\n" +
                "\"751\":lastChild -> \"752\"\n" +
                "\"658\"[label = \" <97> |36|  <155> |72|  <197> |108|  <248> |144|  <306> |180|  <lastChild> |Ls \"];\n" +
                "\"658\":97 -> \"97\"\n" +
                "\"658\":155 -> \"155\"\n" +
                "\"658\":197 -> \"197\"\n" +
                "\"658\":248 -> \"248\"\n" +
                "\"658\":306 -> \"306\"\n" +
                "\"658\":lastChild -> \"358\"\n" +
                "\"97\"[label = \" <13> |6|  <27> |12|  <39> |18|  <40> |24|  <50> |30|  <lastChild> |Ls \"];\n" +
                "\"97\":13 -> \"13\"\n" +
                "\"97\":27 -> \"27\"\n" +
                "\"97\":39 -> \"39\"\n" +
                "\"97\":40 -> \"40\"\n" +
                "\"97\":50 -> \"50\"\n" +
                "\"97\":lastChild -> \"59\"\n" +
                "\"13\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5| \"];\n" +
                "\"27\"[label = \" <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11| \"];\n" +
                "\"39\"[label = \" <12> |12|  <13> |13|  <14> |14|  <15> |15|  <16> |16|  <17> |17| \"];\n" +
                "\"40\"[label = \" <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23| \"];\n" +
                "\"50\"[label = \" <24> |24|  <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"59\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35| \"];\n" +
                "\"155\"[label = \" <60> |42|  <73> |48|  <79> |54|  <88> |60|  <95> |66|  <lastChild> |Ls \"];\n" +
                "\"155\":60 -> \"60\"\n" +
                "\"155\":73 -> \"73\"\n" +
                "\"155\":79 -> \"79\"\n" +
                "\"155\":88 -> \"88\"\n" +
                "\"155\":95 -> \"95\"\n" +
                "\"155\":lastChild -> \"112\"\n" +
                "\"60\"[label = \" <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"73\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47| \"];\n" +
                "\"79\"[label = \" <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53| \"];\n" +
                "\"88\"[label = \" <54> |54|  <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"95\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65| \"];\n" +
                "\"112\"[label = \" <66> |66|  <67> |67|  <68> |68|  <69> |69|  <70> |70|  <71> |71| \"];\n" +
                "\"197\"[label = \" <126> |78|  <127> |84|  <141> |90|  <142> |96|  <156> |102|  <lastChild> |Ls \"];\n" +
                "\"197\":126 -> \"126\"\n" +
                "\"197\":127 -> \"127\"\n" +
                "\"197\":141 -> \"141\"\n" +
                "\"197\":142 -> \"142\"\n" +
                "\"197\":156 -> \"156\"\n" +
                "\"197\":lastChild -> \"157\"\n" +
                "\"126\"[label = \" <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77| \"];\n" +
                "\"127\"[label = \" <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"141\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"142\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95| \"];\n" +
                "\"156\"[label = \" <96> |96|  <97> |97|  <98> |98|  <99> |99|  <100> |100|  <101> |101| \"];\n" +
                "\"157\"[label = \" <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107| \"];\n" +
                "\"248\"[label = \" <171> |114|  <172> |120|  <185> |126|  <198> |132|  <199> |138|  <lastChild> |Ls \"];\n" +
                "\"248\":171 -> \"171\"\n" +
                "\"248\":172 -> \"172\"\n" +
                "\"248\":185 -> \"185\"\n" +
                "\"248\":198 -> \"198\"\n" +
                "\"248\":199 -> \"199\"\n" +
                "\"248\":lastChild -> \"212\"\n" +
                "\"171\"[label = \" <108> |108|  <109> |109|  <110> |110|  <111> |111|  <112> |112|  <113> |113| \"];\n" +
                "\"172\"[label = \" <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"185\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125| \"];\n" +
                "\"198\"[label = \" <126> |126|  <127> |127|  <128> |128|  <129> |129|  <130> |130|  <131> |131| \"];\n" +
                "\"199\"[label = \" <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137| \"];\n" +
                "\"212\"[label = \" <138> |138|  <139> |139|  <140> |140|  <141> |141|  <142> |142|  <143> |143| \"];\n" +
                "\"306\"[label = \" <224> |150|  <225> |156|  <237> |162|  <249> |168|  <250> |174|  <lastChild> |Ls \"];\n" +
                "\"306\":224 -> \"224\"\n" +
                "\"306\":225 -> \"225\"\n" +
                "\"306\":237 -> \"237\"\n" +
                "\"306\":249 -> \"249\"\n" +
                "\"306\":250 -> \"250\"\n" +
                "\"306\":lastChild -> \"262\"\n" +
                "\"224\"[label = \" <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"225\"[label = \" <150> |150|  <151> |151|  <152> |152|  <153> |153|  <154> |154|  <155> |155| \"];\n" +
                "\"237\"[label = \" <156> |156|  <157> |157|  <158> |158|  <159> |159|  <160> |160|  <161> |161| \"];\n" +
                "\"249\"[label = \" <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167| \"];\n" +
                "\"250\"[label = \" <168> |168|  <169> |169|  <170> |170|  <171> |171|  <172> |172|  <173> |173| \"];\n" +
                "\"262\"[label = \" <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179| \"];\n" +
                "\"358\"[label = \" <273> |186|  <274> |192|  <285> |198|  <296> |204|  <307> |210|  <lastChild> |Ls \"];\n" +
                "\"358\":273 -> \"273\"\n" +
                "\"358\":274 -> \"274\"\n" +
                "\"358\":285 -> \"285\"\n" +
                "\"358\":296 -> \"296\"\n" +
                "\"358\":307 -> \"307\"\n" +
                "\"358\":lastChild -> \"308\"\n" +
                "\"273\"[label = \" <180> |180|  <181> |181|  <182> |182|  <183> |183|  <184> |184|  <185> |185| \"];\n" +
                "\"274\"[label = \" <186> |186|  <187> |187|  <188> |188|  <189> |189|  <190> |190|  <191> |191| \"];\n" +
                "\"285\"[label = \" <192> |192|  <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197| \"];\n" +
                "\"296\"[label = \" <198> |198|  <199> |199|  <200> |200|  <201> |201|  <202> |202|  <203> |203| \"];\n" +
                "\"307\"[label = \" <204> |204|  <205> |205|  <206> |206|  <207> |207|  <208> |208|  <209> |209| \"];\n" +
                "\"308\"[label = \" <210> |210|  <211> |211|  <212> |212|  <213> |213|  <214> |214|  <215> |215| \"];\n" +
                "\"752\"[label = \"{ log | { 498-498 }}| <404> |252|  <467> |288|  <521> |324|  <584> |360|  <654> |396|  <719> |432|  <lastChild> |Ls \"];\n" +
                "\"752\":404 -> \"404\"\n" +
                "\"752\":467 -> \"467\"\n" +
                "\"752\":521 -> \"521\"\n" +
                "\"752\":584 -> \"584\"\n" +
                "\"752\":654 -> \"654\"\n" +
                "\"752\":719 -> \"719\"\n" +
                "\"752\":lastChild -> \"753\"\n" +
                "\"404\"[label = \" <319> |222|  <329> |228|  <339> |234|  <349> |240|  <359> |246|  <lastChild> |Ls \"];\n" +
                "\"404\":319 -> \"319\"\n" +
                "\"404\":329 -> \"329\"\n" +
                "\"404\":339 -> \"339\"\n" +
                "\"404\":349 -> \"349\"\n" +
                "\"404\":359 -> \"359\"\n" +
                "\"404\":lastChild -> \"369\"\n" +
                "\"319\"[label = \" <216> |216|  <217> |217|  <218> |218|  <219> |219|  <220> |220|  <221> |221| \"];\n" +
                "\"329\"[label = \" <222> |222|  <223> |223|  <224> |224|  <225> |225|  <226> |226|  <227> |227| \"];\n" +
                "\"339\"[label = \" <228> |228|  <229> |229|  <230> |230|  <231> |231|  <232> |232|  <233> |233| \"];\n" +
                "\"349\"[label = \" <234> |234|  <235> |235|  <236> |236|  <237> |237|  <238> |238|  <239> |239| \"];\n" +
                "\"359\"[label = \" <240> |240|  <241> |241|  <242> |242|  <243> |243|  <244> |244|  <245> |245| \"];\n" +
                "\"369\"[label = \" <246> |246|  <247> |247|  <248> |248|  <249> |249|  <250> |250|  <251> |251| \"];\n" +
                "\"467\"[label = \" <378> |258|  <379> |264|  <396> |270|  <405> |276|  <406> |282|  <lastChild> |Ls \"];\n" +
                "\"467\":378 -> \"378\"\n" +
                "\"467\":379 -> \"379\"\n" +
                "\"467\":396 -> \"396\"\n" +
                "\"467\":405 -> \"405\"\n" +
                "\"467\":406 -> \"406\"\n" +
                "\"467\":lastChild -> \"421\"\n" +
                "\"378\"[label = \" <252> |252|  <253> |253|  <254> |254|  <255> |255|  <256> |256|  <257> |257| \"];\n" +
                "\"379\"[label = \" <258> |258|  <259> |259|  <260> |260|  <261> |261|  <262> |262|  <263> |263| \"];\n" +
                "\"396\"[label = \" <264> |264|  <265> |265|  <266> |266|  <267> |267|  <268> |268|  <269> |269| \"];\n" +
                "\"405\"[label = \" <270> |270|  <271> |271|  <272> |272|  <273> |273|  <274> |274|  <275> |275| \"];\n" +
                "\"406\"[label = \" <276> |276|  <277> |277|  <278> |278|  <279> |279|  <280> |280|  <281> |281| \"];\n" +
                "\"421\"[label = \" <282> |282|  <283> |283|  <284> |284|  <285> |285|  <286> |286|  <287> |287| \"];\n" +
                "\"521\"[label = \" <429> |294|  <437> |300|  <452> |306|  <460> |312|  <468> |318|  <lastChild> |Ls \"];\n" +
                "\"521\":429 -> \"429\"\n" +
                "\"521\":437 -> \"437\"\n" +
                "\"521\":452 -> \"452\"\n" +
                "\"521\":460 -> \"460\"\n" +
                "\"521\":468 -> \"468\"\n" +
                "\"521\":lastChild -> \"476\"\n" +
                "\"429\"[label = \" <288> |288|  <289> |289|  <290> |290|  <291> |291|  <292> |292|  <293> |293| \"];\n" +
                "\"437\"[label = \" <294> |294|  <295> |295|  <296> |296|  <297> |297|  <298> |298|  <299> |299| \"];\n" +
                "\"452\"[label = \" <300> |300|  <301> |301|  <302> |302|  <303> |303|  <304> |304|  <305> |305| \"];\n" +
                "\"460\"[label = \" <306> |306|  <307> |307|  <308> |308|  <309> |309|  <310> |310|  <311> |311| \"];\n" +
                "\"468\"[label = \" <312> |312|  <313> |313|  <314> |314|  <315> |315|  <316> |316|  <317> |317| \"];\n" +
                "\"476\"[label = \" <318> |318|  <319> |319|  <320> |320|  <321> |321|  <322> |322|  <323> |323| \"];\n" +
                "\"584\"[label = \" <488> |330|  <495> |336|  <508> |342|  <515> |348|  <522> |354|  <lastChild> |Ls \"];\n" +
                "\"584\":488 -> \"488\"\n" +
                "\"584\":495 -> \"495\"\n" +
                "\"584\":508 -> \"508\"\n" +
                "\"584\":515 -> \"515\"\n" +
                "\"584\":522 -> \"522\"\n" +
                "\"584\":lastChild -> \"537\"\n" +
                "\"488\"[label = \" <324> |324|  <325> |325|  <326> |326|  <327> |327|  <328> |328|  <329> |329| \"];\n" +
                "\"495\"[label = \" <330> |330|  <331> |331|  <332> |332|  <333> |333|  <334> |334|  <335> |335| \"];\n" +
                "\"508\"[label = \" <336> |336|  <337> |337|  <338> |338|  <339> |339|  <340> |340|  <341> |341| \"];\n" +
                "\"515\"[label = \" <342> |342|  <343> |343|  <344> |344|  <345> |345|  <346> |346|  <347> |347| \"];\n" +
                "\"522\"[label = \" <348> |348|  <349> |349|  <350> |350|  <351> |351|  <352> |352|  <353> |353| \"];\n" +
                "\"537\"[label = \" <354> |354|  <355> |355|  <356> |356|  <357> |357|  <358> |358|  <359> |359| \"];\n" +
                "\"654\"[label = \" <547> |366|  <553> |372|  <568> |378|  <579> |384|  <585> |390|  <lastChild> |Ls \"];\n" +
                "\"654\":547 -> \"547\"\n" +
                "\"654\":553 -> \"553\"\n" +
                "\"654\":568 -> \"568\"\n" +
                "\"654\":579 -> \"579\"\n" +
                "\"654\":585 -> \"585\"\n" +
                "\"654\":lastChild -> \"600\"\n" +
                "\"547\"[label = \" <360> |360|  <361> |361|  <362> |362|  <363> |363|  <364> |364|  <365> |365| \"];\n" +
                "\"553\"[label = \" <366> |366|  <367> |367|  <368> |368|  <369> |369|  <370> |370|  <371> |371| \"];\n" +
                "\"568\"[label = \" <372> |372|  <373> |373|  <374> |374|  <375> |375|  <376> |376|  <377> |377| \"];\n" +
                "\"579\"[label = \" <378> |378|  <379> |379|  <380> |380|  <381> |381|  <382> |382|  <383> |383| \"];\n" +
                "\"585\"[label = \" <384> |384|  <385> |385|  <386> |386|  <387> |387|  <388> |388|  <389> |389| \"];\n" +
                "\"600\"[label = \" <390> |390|  <391> |391|  <392> |392|  <393> |393|  <394> |394|  <395> |395| \"];\n" +
                "\"719\"[label = \" <611> |402|  <619> |408|  <634> |414|  <646> |420|  <655> |426|  <lastChild> |Ls \"];\n" +
                "\"719\":611 -> \"611\"\n" +
                "\"719\":619 -> \"619\"\n" +
                "\"719\":634 -> \"634\"\n" +
                "\"719\":646 -> \"646\"\n" +
                "\"719\":655 -> \"655\"\n" +
                "\"719\":lastChild -> \"674\"\n" +
                "\"611\"[label = \" <396> |396|  <397> |397|  <398> |398|  <399> |399|  <400> |400|  <401> |401| \"];\n" +
                "\"619\"[label = \" <402> |402|  <403> |403|  <404> |404|  <405> |405|  <406> |406|  <407> |407| \"];\n" +
                "\"634\"[label = \" <408> |408|  <409> |409|  <410> |410|  <411> |411|  <412> |412|  <413> |413| \"];\n" +
                "\"646\"[label = \" <414> |414|  <415> |415|  <416> |416|  <417> |417|  <418> |418|  <419> |419| \"];\n" +
                "\"655\"[label = \" <420> |420|  <421> |421|  <422> |422|  <423> |423|  <424> |424|  <425> |425| \"];\n" +
                "\"674\"[label = \" <426> |426|  <427> |427|  <428> |428|  <429> |429|  <430> |430|  <431> |431| \"];\n" +
                "\"753\"[label = \"{ log | { 496-496 }}| <689> |438|  <690> |444|  <705> |450|  <720> |456|  <721> |462|  <737> |468|  <738> |474|  <739> |480|  <754> |486|  <lastChild> |Ls \"];\n" +
                "\"753\":689 -> \"689\"\n" +
                "\"753\":690 -> \"690\"\n" +
                "\"753\":705 -> \"705\"\n" +
                "\"753\":720 -> \"720\"\n" +
                "\"753\":721 -> \"721\"\n" +
                "\"753\":737 -> \"737\"\n" +
                "\"753\":738 -> \"738\"\n" +
                "\"753\":739 -> \"739\"\n" +
                "\"753\":754 -> \"754\"\n" +
                "\"753\":lastChild -> \"755\"\n" +
                "\"689\"[label = \" <432> |432|  <433> |433|  <434> |434|  <435> |435|  <436> |436|  <437> |437| \"];\n" +
                "\"690\"[label = \" <438> |438|  <439> |439|  <440> |440|  <441> |441|  <442> |442|  <443> |443| \"];\n" +
                "\"705\"[label = \" <444> |444|  <445> |445|  <446> |446|  <447> |447|  <448> |448|  <449> |449| \"];\n" +
                "\"720\"[label = \" <450> |450|  <451> |451|  <452> |452|  <453> |453|  <454> |454|  <455> |455| \"];\n" +
                "\"721\"[label = \" <456> |456|  <457> |457|  <458> |458|  <459> |459|  <460> |460|  <461> |461| \"];\n" +
                "\"737\"[label = \" <462> |462|  <463> |463|  <464> |464|  <465> |465|  <466> |466|  <467> |467| \"];\n" +
                "\"738\"[label = \" <468> |468|  <469> |469|  <470> |470|  <471> |471|  <472> |472|  <473> |473| \"];\n" +
                "\"739\"[label = \" <474> |474|  <475> |475|  <476> |476|  <477> |477|  <478> |478|  <479> |479| \"];\n" +
                "\"754\"[label = \" <480> |480|  <481> |481|  <482> |482|  <483> |483|  <484> |484|  <485> |485| \"];\n" +
                "\"755\"[label = \" <486> |486|  <487> |487|  <488> |488|  <489> |489|  <490> |490|  <491> |491|  <492> |492|  <493> |493|  <494> |494|  <495> |495|  <497> |497|  <499> |499| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }

    @Test
    void shouldBeAbleToRetrieveElementsWithLog()
    {
        final int size = 500;
        for (long i = 0; i < size; i++)
        {
            bTree.put(i, i);
        }

        for (long i = 0; i < size; i++)
        {
            assertEquals(i, bTree.get(i));
        }
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
    void shouldNotFailToDeleteNonExistingKeyWithLogWithoutFalsePositives()
    {
        assertEquals(1, bTree.getNodesCount());

        final int numberOfPairs = 200;
        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithoutFalsePositives(i);
        }

        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.put(i, i);
        }

        assertEquals(39, bTree.getNodesCount());

        bTree.removeWithoutFalsePositives(200);
        bTree.removeWithoutFalsePositives(-20);

        assertEquals(39, bTree.getNodesCount());

        assertEquals(InvalidBTreeValues.KEY_NOT_FOUND_VALUE, bTree.get(200));
        assertEquals(InvalidBTreeValues.KEY_NOT_FOUND_VALUE, bTree.get(-20));

        for (long i = 0; i < numberOfPairs; i++)
        {
            assertEquals(i, bTree.get(i));
        }
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWithoutFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.put(i, i);
        }

        assertEquals(39, bTree.getNodesCount());

        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.removeWithoutFalsePositives(i);

            //verify that we still have the rest of the key/values
            if (i + 1 < numberOfPairs)
            {
                for (int j = i + 1; j < numberOfPairs; j++)
                {
                    assertEquals(j, bTree.get(j));
                }
            }
        }

        assertEquals(1L, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"762\"[label = \"\"];\n" +
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
            bTree.remove(i);
        }

        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.put(i, i);
        }

        assertEquals(39, bTree.getNodesCount());

        bTree.remove(200);
        bTree.remove(-20);

        assertEquals(39, bTree.getNodesCount());

        assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(200));
        assertEquals(KEY_NOT_FOUND_VALUE, bTree.get(-20));

        for (long i = 0; i < numberOfPairs; i++)
        {
            assertEquals(i, bTree.get(i));
        }
    }

    @Test
    void shouldBeAbleToDeleteElementsWithLogWitFalsePositives()
    {
        final int numberOfPairs = 200;
        for (long i = 0; i < numberOfPairs; i++)
        {
            bTree.put(i, i);
        }

        for (int i = 0; i < numberOfPairs; i++)
        {
            bTree.remove(i);
        }

        assertEquals(5, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"552\"[label = \"{ log | { 193--1 |  194--1 |  195--1 |  196--1 |  197--1 |  198--1 |  199--1 }}| <479> |144|  <lastChild> |Ls \"];\n" +
                "\"552\":479 -> \"479\"\n" +
                "\"552\":lastChild -> \"542\"\n" +
                "\"479\"[label = \"\"];\n" +
                "\"542\"[label = \"{ log | { 191--1 }}| <543> |192|  <lastChild> |Ls \"];\n" +
                "\"542\":543 -> \"543\"\n" +
                "\"542\":lastChild -> \"545\"\n" +
                "\"543\"[label = \" <191> |191| \"];\n" +
                "\"545\"[label = \" <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197|  <198> |198|  <199> |199| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}