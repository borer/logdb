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
import static org.logdb.support.TestUtils.NODE_LOG_PERCENTAGE;
import static org.logdb.support.TestUtils.createInitialRootReference;
import static org.logdb.support.TestUtils.createRootIndex;

class BTreeWithLogTest
{
    private static final int PAGE_SIZE = 256;
    private static final int EXPECTED_NODES = 41;
    private BTreeWithLog bTree;

    @BeforeEach
    void setUp()
    {
        final Storage treeStorage = new MemoryStorage(TestUtils.BYTE_ORDER, PAGE_SIZE, MEMORY_CHUNK_SIZE);
        final RootIndex rootIndex = createRootIndex(PAGE_SIZE);

        NodesManager nodesManager = new NodesManager(treeStorage, rootIndex, true);
        bTree = new BTreeWithLog(
                nodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManager),
                NODE_LOG_PERCENTAGE);
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
                "\"851\"[label = \"{ log | { 498-498 |  499-499 }}| <308> |96|  <491> |192|  <664> |288|  <837> |384|  <lastChild> |Ls \"];\n" +
                "\"851\":308 -> \"308\"\n" +
                "\"851\":491 -> \"491\"\n" +
                "\"851\":664 -> \"664\"\n" +
                "\"851\":837 -> \"837\"\n" +
                "\"851\":lastChild -> \"847\"\n" +
                "\"308\"[label = \" <74> |24|  <108> |48|  <148> |72|  <lastChild> |Ls \"];\n" +
                "\"308\":74 -> \"74\"\n" +
                "\"308\":108 -> \"108\"\n" +
                "\"308\":148 -> \"148\"\n" +
                "\"308\":lastChild -> \"188\"\n" +
                "\"74\"[label = \" <13> |6|  <27> |12|  <34> |18|  <lastChild> |Ls \"];\n" +
                "\"74\":13 -> \"13\"\n" +
                "\"74\":27 -> \"27\"\n" +
                "\"74\":34 -> \"34\"\n" +
                "\"74\":lastChild -> \"41\"\n" +
                "\"13\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5| \"];\n" +
                "\"27\"[label = \" <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11| \"];\n" +
                "\"34\"[label = \" <12> |12|  <13> |13|  <14> |14|  <15> |15|  <16> |16|  <17> |17| \"];\n" +
                "\"41\"[label = \" <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23| \"];\n" +
                "\"108\"[label = \" <48> |30|  <55> |36|  <68> |42|  <lastChild> |Ls \"];\n" +
                "\"108\":48 -> \"48\"\n" +
                "\"108\":55 -> \"55\"\n" +
                "\"108\":68 -> \"68\"\n" +
                "\"108\":lastChild -> \"77\"\n" +
                "\"48\"[label = \" <24> |24|  <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"55\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35| \"];\n" +
                "\"68\"[label = \" <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"77\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47| \"];\n" +
                "\"148\"[label = \" <85> |54|  <93> |60|  <101> |66|  <lastChild> |Ls \"];\n" +
                "\"148\":85 -> \"85\"\n" +
                "\"148\":93 -> \"93\"\n" +
                "\"148\":101 -> \"101\"\n" +
                "\"148\":lastChild -> \"117\"\n" +
                "\"85\"[label = \" <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53| \"];\n" +
                "\"93\"[label = \" <54> |54|  <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"101\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65| \"];\n" +
                "\"117\"[label = \" <66> |66|  <67> |67|  <68> |68|  <69> |69|  <70> |70|  <71> |71| \"];\n" +
                "\"188\"[label = \" <125> |78|  <133> |84|  <141> |90|  <lastChild> |Ls \"];\n" +
                "\"188\":125 -> \"125\"\n" +
                "\"188\":133 -> \"133\"\n" +
                "\"188\":141 -> \"141\"\n" +
                "\"188\":lastChild -> \"150\"\n" +
                "\"125\"[label = \" <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77| \"];\n" +
                "\"133\"[label = \" <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"141\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"150\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95| \"];\n" +
                "\"491\"[label = \" <228> |120|  <268> |144|  <301> |168|  <lastChild> |Ls \"];\n" +
                "\"491\":228 -> \"228\"\n" +
                "\"491\":268 -> \"268\"\n" +
                "\"491\":301 -> \"301\"\n" +
                "\"491\":lastChild -> \"347\"\n" +
                "\"228\"[label = \" <165> |102|  <173> |108|  <181> |114|  <lastChild> |Ls \"];\n" +
                "\"228\":165 -> \"165\"\n" +
                "\"228\":173 -> \"173\"\n" +
                "\"228\":181 -> \"181\"\n" +
                "\"228\":lastChild -> \"190\"\n" +
                "\"165\"[label = \" <96> |96|  <97> |97|  <98> |98|  <99> |99|  <100> |100|  <101> |101| \"];\n" +
                "\"173\"[label = \" <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107| \"];\n" +
                "\"181\"[label = \" <108> |108|  <109> |109|  <110> |110|  <111> |111|  <112> |112|  <113> |113| \"];\n" +
                "\"190\"[label = \" <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"268\"[label = \" <198> |126|  <213> |132|  <221> |138|  <lastChild> |Ls \"];\n" +
                "\"268\":198 -> \"198\"\n" +
                "\"268\":213 -> \"213\"\n" +
                "\"268\":221 -> \"221\"\n" +
                "\"268\":lastChild -> \"230\"\n" +
                "\"198\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125| \"];\n" +
                "\"213\"[label = \" <126> |126|  <127> |127|  <128> |128|  <129> |129|  <130> |130|  <131> |131| \"];\n" +
                "\"221\"[label = \" <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137| \"];\n" +
                "\"230\"[label = \" <138> |138|  <139> |139|  <140> |140|  <141> |141|  <142> |142|  <143> |143| \"];\n" +
                "\"301\"[label = \" <238> |150|  <246> |156|  <261> |162|  <lastChild> |Ls \"];\n" +
                "\"301\":238 -> \"238\"\n" +
                "\"301\":246 -> \"246\"\n" +
                "\"301\":261 -> \"261\"\n" +
                "\"301\":lastChild -> \"270\"\n" +
                "\"238\"[label = \" <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"246\"[label = \" <150> |150|  <151> |151|  <152> |152|  <153> |153|  <154> |154|  <155> |155| \"];\n" +
                "\"261\"[label = \" <156> |156|  <157> |157|  <158> |158|  <159> |159|  <160> |160|  <161> |161| \"];\n" +
                "\"270\"[label = \" <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167| \"];\n" +
                "\"347\"[label = \" <278> |174|  <286> |180|  <294> |186|  <lastChild> |Ls \"];\n" +
                "\"347\":278 -> \"278\"\n" +
                "\"347\":286 -> \"286\"\n" +
                "\"347\":294 -> \"294\"\n" +
                "\"347\":lastChild -> \"312\"\n" +
                "\"278\"[label = \" <168> |168|  <169> |169|  <170> |170|  <171> |171|  <172> |172|  <173> |173| \"];\n" +
                "\"286\"[label = \" <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179| \"];\n" +
                "\"294\"[label = \" <180> |180|  <181> |181|  <182> |182|  <183> |183|  <184> |184|  <185> |185| \"];\n" +
                "\"312\"[label = \" <186> |186|  <187> |187|  <188> |188|  <189> |189|  <190> |190|  <191> |191| \"];\n" +
                "\"664\"[label = \" <392> |216|  <437> |240|  <482> |264|  <lastChild> |Ls \"];\n" +
                "\"664\":392 -> \"392\"\n" +
                "\"664\":437 -> \"437\"\n" +
                "\"664\":482 -> \"482\"\n" +
                "\"664\":lastChild -> \"520\"\n" +
                "\"392\"[label = \" <321> |198|  <330> |204|  <339> |210|  <lastChild> |Ls \"];\n" +
                "\"392\":321 -> \"321\"\n" +
                "\"392\":330 -> \"330\"\n" +
                "\"392\":339 -> \"339\"\n" +
                "\"392\":lastChild -> \"349\"\n" +
                "\"321\"[label = \" <192> |192|  <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197| \"];\n" +
                "\"330\"[label = \" <198> |198|  <199> |199|  <200> |200|  <201> |201|  <202> |202|  <203> |203| \"];\n" +
                "\"339\"[label = \" <204> |204|  <205> |205|  <206> |206|  <207> |207|  <208> |208|  <209> |209| \"];\n" +
                "\"349\"[label = \" <210> |210|  <211> |211|  <212> |212|  <213> |213|  <214> |214|  <215> |215| \"];\n" +
                "\"437\"[label = \" <366> |222|  <375> |228|  <384> |234|  <lastChild> |Ls \"];\n" +
                "\"437\":366 -> \"366\"\n" +
                "\"437\":375 -> \"375\"\n" +
                "\"437\":384 -> \"384\"\n" +
                "\"437\":lastChild -> \"394\"\n" +
                "\"366\"[label = \" <216> |216|  <217> |217|  <218> |218|  <219> |219|  <220> |220|  <221> |221| \"];\n" +
                "\"375\"[label = \" <222> |222|  <223> |223|  <224> |224|  <225> |225|  <226> |226|  <227> |227| \"];\n" +
                "\"384\"[label = \" <228> |228|  <229> |229|  <230> |230|  <231> |231|  <232> |232|  <233> |233| \"];\n" +
                "\"394\"[label = \" <234> |234|  <235> |235|  <236> |236|  <237> |237|  <238> |238|  <239> |239| \"];\n" +
                "\"482\"[label = \" <403> |246|  <420> |252|  <429> |258|  <lastChild> |Ls \"];\n" +
                "\"482\":403 -> \"403\"\n" +
                "\"482\":420 -> \"420\"\n" +
                "\"482\":429 -> \"429\"\n" +
                "\"482\":lastChild -> \"439\"\n" +
                "\"403\"[label = \" <240> |240|  <241> |241|  <242> |242|  <243> |243|  <244> |244|  <245> |245| \"];\n" +
                "\"420\"[label = \" <246> |246|  <247> |247|  <248> |248|  <249> |249|  <250> |250|  <251> |251| \"];\n" +
                "\"429\"[label = \" <252> |252|  <253> |253|  <254> |254|  <255> |255|  <256> |256|  <257> |257| \"];\n" +
                "\"439\"[label = \" <258> |258|  <259> |259|  <260> |260|  <261> |261|  <262> |262|  <263> |263| \"];\n" +
                "\"520\"[label = \" <448> |270|  <457> |276|  <474> |282|  <lastChild> |Ls \"];\n" +
                "\"520\":448 -> \"448\"\n" +
                "\"520\":457 -> \"457\"\n" +
                "\"520\":474 -> \"474\"\n" +
                "\"520\":lastChild -> \"484\"\n" +
                "\"448\"[label = \" <264> |264|  <265> |265|  <266> |266|  <267> |267|  <268> |268|  <269> |269| \"];\n" +
                "\"457\"[label = \" <270> |270|  <271> |271|  <272> |272|  <273> |273|  <274> |274|  <275> |275| \"];\n" +
                "\"474\"[label = \" <276> |276|  <277> |277|  <278> |278|  <279> |279|  <280> |280|  <281> |281| \"];\n" +
                "\"484\"[label = \" <282> |282|  <283> |283|  <284> |284|  <285> |285|  <286> |286|  <287> |287| \"];\n" +
                "\"837\"[label = \" <565> |312|  <610> |336|  <655> |360|  <lastChild> |Ls \"];\n" +
                "\"837\":565 -> \"565\"\n" +
                "\"837\":610 -> \"610\"\n" +
                "\"837\":655 -> \"655\"\n" +
                "\"837\":lastChild -> \"701\"\n" +
                "\"565\"[label = \" <494> |294|  <503> |300|  <512> |306|  <lastChild> |Ls \"];\n" +
                "\"565\":494 -> \"494\"\n" +
                "\"565\":503 -> \"503\"\n" +
                "\"565\":512 -> \"512\"\n" +
                "\"565\":lastChild -> \"530\"\n" +
                "\"494\"[label = \" <288> |288|  <289> |289|  <290> |290|  <291> |291|  <292> |292|  <293> |293| \"];\n" +
                "\"503\"[label = \" <294> |294|  <295> |295|  <296> |296|  <297> |297|  <298> |298|  <299> |299| \"];\n" +
                "\"512\"[label = \" <300> |300|  <301> |301|  <302> |302|  <303> |303|  <304> |304|  <305> |305| \"];\n" +
                "\"530\"[label = \" <306> |306|  <307> |307|  <308> |308|  <309> |309|  <310> |310|  <311> |311| \"];\n" +
                "\"610\"[label = \" <539> |318|  <548> |324|  <557> |330|  <lastChild> |Ls \"];\n" +
                "\"610\":539 -> \"539\"\n" +
                "\"610\":548 -> \"548\"\n" +
                "\"610\":557 -> \"557\"\n" +
                "\"610\":lastChild -> \"567\"\n" +
                "\"539\"[label = \" <312> |312|  <313> |313|  <314> |314|  <315> |315|  <316> |316|  <317> |317| \"];\n" +
                "\"548\"[label = \" <318> |318|  <319> |319|  <320> |320|  <321> |321|  <322> |322|  <323> |323| \"];\n" +
                "\"557\"[label = \" <324> |324|  <325> |325|  <326> |326|  <327> |327|  <328> |328|  <329> |329| \"];\n" +
                "\"567\"[label = \" <330> |330|  <331> |331|  <332> |332|  <333> |333|  <334> |334|  <335> |335| \"];\n" +
                "\"655\"[label = \" <584> |342|  <593> |348|  <602> |354|  <lastChild> |Ls \"];\n" +
                "\"655\":584 -> \"584\"\n" +
                "\"655\":593 -> \"593\"\n" +
                "\"655\":602 -> \"602\"\n" +
                "\"655\":lastChild -> \"612\"\n" +
                "\"584\"[label = \" <336> |336|  <337> |337|  <338> |338|  <339> |339|  <340> |340|  <341> |341| \"];\n" +
                "\"593\"[label = \" <342> |342|  <343> |343|  <344> |344|  <345> |345|  <346> |346|  <347> |347| \"];\n" +
                "\"602\"[label = \" <348> |348|  <349> |349|  <350> |350|  <351> |351|  <352> |352|  <353> |353| \"];\n" +
                "\"612\"[label = \" <354> |354|  <355> |355|  <356> |356|  <357> |357|  <358> |358|  <359> |359| \"];\n" +
                "\"701\"[label = \" <621> |366|  <638> |372|  <647> |378|  <lastChild> |Ls \"];\n" +
                "\"701\":621 -> \"621\"\n" +
                "\"701\":638 -> \"638\"\n" +
                "\"701\":647 -> \"647\"\n" +
                "\"701\":lastChild -> \"657\"\n" +
                "\"621\"[label = \" <360> |360|  <361> |361|  <362> |362|  <363> |363|  <364> |364|  <365> |365| \"];\n" +
                "\"638\"[label = \" <366> |366|  <367> |367|  <368> |368|  <369> |369|  <370> |370|  <371> |371| \"];\n" +
                "\"647\"[label = \" <372> |372|  <373> |373|  <374> |374|  <375> |375|  <376> |376|  <377> |377| \"];\n" +
                "\"657\"[label = \" <378> |378|  <379> |379|  <380> |380|  <381> |381|  <382> |382|  <383> |383| \"];\n" +
                "\"847\"[label = \" <738> |408|  <783> |432|  <828> |456|  <lastChild> |Ls \"];\n" +
                "\"847\":738 -> \"738\"\n" +
                "\"847\":783 -> \"783\"\n" +
                "\"847\":828 -> \"828\"\n" +
                "\"847\":lastChild -> \"848\"\n" +
                "\"738\"[label = \" <667> |390|  <676> |396|  <693> |402|  <lastChild> |Ls \"];\n" +
                "\"738\":667 -> \"667\"\n" +
                "\"738\":676 -> \"676\"\n" +
                "\"738\":693 -> \"693\"\n" +
                "\"738\":lastChild -> \"703\"\n" +
                "\"667\"[label = \" <384> |384|  <385> |385|  <386> |386|  <387> |387|  <388> |388|  <389> |389| \"];\n" +
                "\"676\"[label = \" <390> |390|  <391> |391|  <392> |392|  <393> |393|  <394> |394|  <395> |395| \"];\n" +
                "\"693\"[label = \" <396> |396|  <397> |397|  <398> |398|  <399> |399|  <400> |400|  <401> |401| \"];\n" +
                "\"703\"[label = \" <402> |402|  <403> |403|  <404> |404|  <405> |405|  <406> |406|  <407> |407| \"];\n" +
                "\"783\"[label = \" <712> |414|  <721> |420|  <730> |426|  <lastChild> |Ls \"];\n" +
                "\"783\":712 -> \"712\"\n" +
                "\"783\":721 -> \"721\"\n" +
                "\"783\":730 -> \"730\"\n" +
                "\"783\":lastChild -> \"748\"\n" +
                "\"712\"[label = \" <408> |408|  <409> |409|  <410> |410|  <411> |411|  <412> |412|  <413> |413| \"];\n" +
                "\"721\"[label = \" <414> |414|  <415> |415|  <416> |416|  <417> |417|  <418> |418|  <419> |419| \"];\n" +
                "\"730\"[label = \" <420> |420|  <421> |421|  <422> |422|  <423> |423|  <424> |424|  <425> |425| \"];\n" +
                "\"748\"[label = \" <426> |426|  <427> |427|  <428> |428|  <429> |429|  <430> |430|  <431> |431| \"];\n" +
                "\"828\"[label = \" <757> |438|  <766> |444|  <775> |450|  <lastChild> |Ls \"];\n" +
                "\"828\":757 -> \"757\"\n" +
                "\"828\":766 -> \"766\"\n" +
                "\"828\":775 -> \"775\"\n" +
                "\"828\":lastChild -> \"785\"\n" +
                "\"757\"[label = \" <432> |432|  <433> |433|  <434> |434|  <435> |435|  <436> |436|  <437> |437| \"];\n" +
                "\"766\"[label = \" <438> |438|  <439> |439|  <440> |440|  <441> |441|  <442> |442|  <443> |443| \"];\n" +
                "\"775\"[label = \" <444> |444|  <445> |445|  <446> |446|  <447> |447|  <448> |448|  <449> |449| \"];\n" +
                "\"785\"[label = \" <450> |450|  <451> |451|  <452> |452|  <453> |453|  <454> |454|  <455> |455| \"];\n" +
                "\"848\"[label = \" <802> |462|  <811> |468|  <820> |474|  <830> |480|  <840> |486|  <lastChild> |Ls \"];\n" +
                "\"848\":802 -> \"802\"\n" +
                "\"848\":811 -> \"811\"\n" +
                "\"848\":820 -> \"820\"\n" +
                "\"848\":830 -> \"830\"\n" +
                "\"848\":840 -> \"840\"\n" +
                "\"848\":lastChild -> \"849\"\n" +
                "\"802\"[label = \" <456> |456|  <457> |457|  <458> |458|  <459> |459|  <460> |460|  <461> |461| \"];\n" +
                "\"811\"[label = \" <462> |462|  <463> |463|  <464> |464|  <465> |465|  <466> |466|  <467> |467| \"];\n" +
                "\"820\"[label = \" <468> |468|  <469> |469|  <470> |470|  <471> |471|  <472> |472|  <473> |473| \"];\n" +
                "\"830\"[label = \" <474> |474|  <475> |475|  <476> |476|  <477> |477|  <478> |478|  <479> |479| \"];\n" +
                "\"840\"[label = \" <480> |480|  <481> |481|  <482> |482|  <483> |483|  <484> |484|  <485> |485| \"];\n" +
                "\"849\"[label = \" <486> |486|  <487> |487|  <488> |488|  <489> |489|  <490> |490|  <491> |491|  <492> |492|  <493> |493|  <494> |494|  <495> |495|  <496> |496|  <497> |497| \"];\n" +
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

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        bTree.removeWithoutFalsePositives(200);
        bTree.removeWithoutFalsePositives(-20);

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

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

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

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

        assertEquals(3L, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"817\"[label = \" <739> |168|  <lastChild> |Ls \"];\n" +
                "\"817\":739 -> \"739\"\n" +
                "\"817\":lastChild -> \"814\"\n" +
                "\"739\"[label = \"\"];\n" +
                "\"814\"[label = \"\"];\n" +
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

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

        bTree.remove(200);
        bTree.remove(-20);

        assertEquals(EXPECTED_NODES, bTree.getNodesCount());

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

        assertEquals(3, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"617\"[label = \"{ log | { 196--1 |  197--1 |  198--1 |  199--1 }}| <613> |192|  <lastChild> |Ls \"];\n" +
                "\"617\":613 -> \"613\"\n" +
                "\"617\":lastChild -> \"612\"\n" +
                "\"613\"[label = \"\"];\n" +
                "\"612\"[label = \" <196> |196|  <197> |197|  <198> |198|  <199> |199| \"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}