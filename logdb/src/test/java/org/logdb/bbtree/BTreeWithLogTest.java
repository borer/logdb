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
    private static final int MAX_LOG_SIZE = 76;
    private static final int EXPECTED_NODES = 43;
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
                "\"859\"[label = \" <253> |96|  <431> |192|  <604> |288|  <777> |384|  <lastChild> |Ls \"];\n" +
                "\"859\":253 -> \"253\"\n" +
                "\"859\":431 -> \"431\"\n" +
                "\"859\":604 -> \"604\"\n" +
                "\"859\":777 -> \"777\"\n" +
                "\"859\":lastChild -> \"860\"\n" +
                "\"253\"[label = \" <57> |24|  <96> |48|  <136> |72|  <lastChild> |Ls \"];\n" +
                "\"253\":57 -> \"57\"\n" +
                "\"253\":96 -> \"96\"\n" +
                "\"253\":136 -> \"136\"\n" +
                "\"253\":lastChild -> \"176\"\n" +
                "\"57\"[label = \" <13> |6|  <27> |12|  <34> |18|  <lastChild> |Ls \"];\n" +
                "\"57\":13 -> \"13\"\n" +
                "\"57\":27 -> \"27\"\n" +
                "\"57\":34 -> \"34\"\n" +
                "\"57\":lastChild -> \"41\"\n" +
                "\"13\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5| \"];\n" +
                "\"27\"[label = \" <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11| \"];\n" +
                "\"34\"[label = \" <12> |12|  <13> |13|  <14> |14|  <15> |15|  <16> |16|  <17> |17| \"];\n" +
                "\"41\"[label = \" <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23| \"];\n" +
                "\"96\"[label = \" <48> |30|  <55> |36|  <73> |42|  <lastChild> |Ls \"];\n" +
                "\"96\":48 -> \"48\"\n" +
                "\"96\":55 -> \"55\"\n" +
                "\"96\":73 -> \"73\"\n" +
                "\"96\":lastChild -> \"81\"\n" +
                "\"48\"[label = \" <24> |24|  <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                "\"55\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35| \"];\n" +
                "\"73\"[label = \" <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                "\"81\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47| \"];\n" +
                "\"136\"[label = \" <89> |54|  <97> |60|  <106> |66|  <lastChild> |Ls \"];\n" +
                "\"136\":89 -> \"89\"\n" +
                "\"136\":97 -> \"97\"\n" +
                "\"136\":106 -> \"106\"\n" +
                "\"136\":lastChild -> \"121\"\n" +
                "\"89\"[label = \" <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53| \"];\n" +
                "\"97\"[label = \" <54> |54|  <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                "\"106\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65| \"];\n" +
                "\"121\"[label = \" <66> |66|  <67> |67|  <68> |68|  <69> |69|  <70> |70|  <71> |71| \"];\n" +
                "\"176\"[label = \" <129> |78|  <137> |84|  <146> |90|  <lastChild> |Ls \"];\n" +
                "\"176\":129 -> \"129\"\n" +
                "\"176\":137 -> \"137\"\n" +
                "\"176\":146 -> \"146\"\n" +
                "\"176\":lastChild -> \"154\"\n" +
                "\"129\"[label = \" <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77| \"];\n" +
                "\"137\"[label = \" <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                "\"146\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89| \"];\n" +
                "\"154\"[label = \" <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95| \"];\n" +
                "\"431\"[label = \" <216> |120|  <249> |144|  <297> |168|  <lastChild> |Ls \"];\n" +
                "\"431\":216 -> \"216\"\n" +
                "\"431\":249 -> \"249\"\n" +
                "\"431\":297 -> \"297\"\n" +
                "\"431\":lastChild -> \"342\"\n" +
                "\"216\"[label = \" <169> |102|  <177> |108|  <186> |114|  <lastChild> |Ls \"];\n" +
                "\"216\":169 -> \"169\"\n" +
                "\"216\":177 -> \"177\"\n" +
                "\"216\":186 -> \"186\"\n" +
                "\"216\":lastChild -> \"194\"\n" +
                "\"169\"[label = \" <96> |96|  <97> |97|  <98> |98|  <99> |99|  <100> |100|  <101> |101| \"];\n" +
                "\"177\"[label = \" <102> |102|  <103> |103|  <104> |104|  <105> |105|  <106> |106|  <107> |107| \"];\n" +
                "\"186\"[label = \" <108> |108|  <109> |109|  <110> |110|  <111> |111|  <112> |112|  <113> |113| \"];\n" +
                "\"194\"[label = \" <114> |114|  <115> |115|  <116> |116|  <117> |117|  <118> |118|  <119> |119| \"];\n" +
                "\"249\"[label = \" <202> |126|  <217> |132|  <226> |138|  <lastChild> |Ls \"];\n" +
                "\"249\":202 -> \"202\"\n" +
                "\"249\":217 -> \"217\"\n" +
                "\"249\":226 -> \"226\"\n" +
                "\"249\":lastChild -> \"234\"\n" +
                "\"202\"[label = \" <120> |120|  <121> |121|  <122> |122|  <123> |123|  <124> |124|  <125> |125| \"];\n" +
                "\"217\"[label = \" <126> |126|  <127> |127|  <128> |128|  <129> |129|  <130> |130|  <131> |131| \"];\n" +
                "\"226\"[label = \" <132> |132|  <133> |133|  <134> |134|  <135> |135|  <136> |136|  <137> |137| \"];\n" +
                "\"234\"[label = \" <138> |138|  <139> |139|  <140> |140|  <141> |141|  <142> |142|  <143> |143| \"];\n" +
                "\"297\"[label = \" <242> |150|  <250> |156|  <271> |162|  <lastChild> |Ls \"];\n" +
                "\"297\":242 -> \"242\"\n" +
                "\"297\":250 -> \"250\"\n" +
                "\"297\":271 -> \"271\"\n" +
                "\"297\":lastChild -> \"280\"\n" +
                "\"242\"[label = \" <144> |144|  <145> |145|  <146> |146|  <147> |147|  <148> |148|  <149> |149| \"];\n" +
                "\"250\"[label = \" <150> |150|  <151> |151|  <152> |152|  <153> |153|  <154> |154|  <155> |155| \"];\n" +
                "\"271\"[label = \" <156> |156|  <157> |157|  <158> |158|  <159> |159|  <160> |160|  <161> |161| \"];\n" +
                "\"280\"[label = \" <162> |162|  <163> |163|  <164> |164|  <165> |165|  <166> |166|  <167> |167| \"];\n" +
                "\"342\"[label = \" <289> |174|  <298> |180|  <308> |186|  <lastChild> |Ls \"];\n" +
                "\"342\":289 -> \"289\"\n" +
                "\"342\":298 -> \"298\"\n" +
                "\"342\":308 -> \"308\"\n" +
                "\"342\":lastChild -> \"325\"\n" +
                "\"289\"[label = \" <168> |168|  <169> |169|  <170> |170|  <171> |171|  <172> |172|  <173> |173| \"];\n" +
                "\"298\"[label = \" <174> |174|  <175> |175|  <176> |176|  <177> |177|  <178> |178|  <179> |179| \"];\n" +
                "\"308\"[label = \" <180> |180|  <181> |181|  <182> |182|  <183> |183|  <184> |184|  <185> |185| \"];\n" +
                "\"325\"[label = \" <186> |186|  <187> |187|  <188> |188|  <189> |189|  <190> |190|  <191> |191| \"];\n" +
                "\"604\"[label = \" <387> |216|  <432> |240|  <470> |264|  <lastChild> |Ls \"];\n" +
                "\"604\":387 -> \"387\"\n" +
                "\"604\":432 -> \"432\"\n" +
                "\"604\":470 -> \"470\"\n" +
                "\"604\":lastChild -> \"515\"\n" +
                "\"387\"[label = \" <334> |198|  <343> |204|  <353> |210|  <lastChild> |Ls \"];\n" +
                "\"387\":334 -> \"334\"\n" +
                "\"387\":343 -> \"343\"\n" +
                "\"387\":353 -> \"353\"\n" +
                "\"387\":lastChild -> \"362\"\n" +
                "\"334\"[label = \" <192> |192|  <193> |193|  <194> |194|  <195> |195|  <196> |196|  <197> |197| \"];\n" +
                "\"343\"[label = \" <198> |198|  <199> |199|  <200> |200|  <201> |201|  <202> |202|  <203> |203| \"];\n" +
                "\"353\"[label = \" <204> |204|  <205> |205|  <206> |206|  <207> |207|  <208> |208|  <209> |209| \"];\n" +
                "\"362\"[label = \" <210> |210|  <211> |211|  <212> |212|  <213> |213|  <214> |214|  <215> |215| \"];\n" +
                "\"432\"[label = \" <379> |222|  <388> |228|  <398> |234|  <lastChild> |Ls \"];\n" +
                "\"432\":379 -> \"379\"\n" +
                "\"432\":388 -> \"388\"\n" +
                "\"432\":398 -> \"398\"\n" +
                "\"432\":lastChild -> \"407\"\n" +
                "\"379\"[label = \" <216> |216|  <217> |217|  <218> |218|  <219> |219|  <220> |220|  <221> |221| \"];\n" +
                "\"388\"[label = \" <222> |222|  <223> |223|  <224> |224|  <225> |225|  <226> |226|  <227> |227| \"];\n" +
                "\"398\"[label = \" <228> |228|  <229> |229|  <230> |230|  <231> |231|  <232> |232|  <233> |233| \"];\n" +
                "\"407\"[label = \" <234> |234|  <235> |235|  <236> |236|  <237> |237|  <238> |238|  <239> |239| \"];\n" +
                "\"470\"[label = \" <416> |246|  <433> |252|  <444> |258|  <lastChild> |Ls \"];\n" +
                "\"470\":416 -> \"416\"\n" +
                "\"470\":433 -> \"433\"\n" +
                "\"470\":444 -> \"444\"\n" +
                "\"470\":lastChild -> \"453\"\n" +
                "\"416\"[label = \" <240> |240|  <241> |241|  <242> |242|  <243> |243|  <244> |244|  <245> |245| \"];\n" +
                "\"433\"[label = \" <246> |246|  <247> |247|  <248> |248|  <249> |249|  <250> |250|  <251> |251| \"];\n" +
                "\"444\"[label = \" <252> |252|  <253> |253|  <254> |254|  <255> |255|  <256> |256|  <257> |257| \"];\n" +
                "\"453\"[label = \" <258> |258|  <259> |259|  <260> |260|  <261> |261|  <262> |262|  <263> |263| \"];\n" +
                "\"515\"[label = \" <462> |270|  <471> |276|  <489> |282|  <lastChild> |Ls \"];\n" +
                "\"515\":462 -> \"462\"\n" +
                "\"515\":471 -> \"471\"\n" +
                "\"515\":489 -> \"489\"\n" +
                "\"515\":lastChild -> \"498\"\n" +
                "\"462\"[label = \" <264> |264|  <265> |265|  <266> |266|  <267> |267|  <268> |268|  <269> |269| \"];\n" +
                "\"471\"[label = \" <270> |270|  <271> |271|  <272> |272|  <273> |273|  <274> |274|  <275> |275| \"];\n" +
                "\"489\"[label = \" <276> |276|  <277> |277|  <278> |278|  <279> |279|  <280> |280|  <281> |281| \"];\n" +
                "\"498\"[label = \" <282> |282|  <283> |283|  <284> |284|  <285> |285|  <286> |286|  <287> |287| \"];\n" +
                "\"777\"[label = \" <560> |312|  <605> |336|  <651> |360|  <lastChild> |Ls \"];\n" +
                "\"777\":560 -> \"560\"\n" +
                "\"777\":605 -> \"605\"\n" +
                "\"777\":651 -> \"651\"\n" +
                "\"777\":lastChild -> \"688\"\n" +
                "\"560\"[label = \" <507> |294|  <516> |300|  <526> |306|  <lastChild> |Ls \"];\n" +
                "\"560\":507 -> \"507\"\n" +
                "\"560\":516 -> \"516\"\n" +
                "\"560\":526 -> \"526\"\n" +
                "\"560\":lastChild -> \"543\"\n" +
                "\"507\"[label = \" <288> |288|  <289> |289|  <290> |290|  <291> |291|  <292> |292|  <293> |293| \"];\n" +
                "\"516\"[label = \" <294> |294|  <295> |295|  <296> |296|  <297> |297|  <298> |298|  <299> |299| \"];\n" +
                "\"526\"[label = \" <300> |300|  <301> |301|  <302> |302|  <303> |303|  <304> |304|  <305> |305| \"];\n" +
                "\"543\"[label = \" <306> |306|  <307> |307|  <308> |308|  <309> |309|  <310> |310|  <311> |311| \"];\n" +
                "\"605\"[label = \" <552> |318|  <561> |324|  <571> |330|  <lastChild> |Ls \"];\n" +
                "\"605\":552 -> \"552\"\n" +
                "\"605\":561 -> \"561\"\n" +
                "\"605\":571 -> \"571\"\n" +
                "\"605\":lastChild -> \"580\"\n" +
                "\"552\"[label = \" <312> |312|  <313> |313|  <314> |314|  <315> |315|  <316> |316|  <317> |317| \"];\n" +
                "\"561\"[label = \" <318> |318|  <319> |319|  <320> |320|  <321> |321|  <322> |322|  <323> |323| \"];\n" +
                "\"571\"[label = \" <324> |324|  <325> |325|  <326> |326|  <327> |327|  <328> |328|  <329> |329| \"];\n" +
                "\"580\"[label = \" <330> |330|  <331> |331|  <332> |332|  <333> |333|  <334> |334|  <335> |335| \"];\n" +
                "\"651\"[label = \" <597> |342|  <606> |348|  <617> |354|  <lastChild> |Ls \"];\n" +
                "\"651\":597 -> \"597\"\n" +
                "\"651\":606 -> \"606\"\n" +
                "\"651\":617 -> \"617\"\n" +
                "\"651\":lastChild -> \"626\"\n" +
                "\"597\"[label = \" <336> |336|  <337> |337|  <338> |338|  <339> |339|  <340> |340|  <341> |341| \"];\n" +
                "\"606\"[label = \" <342> |342|  <343> |343|  <344> |344|  <345> |345|  <346> |346|  <347> |347| \"];\n" +
                "\"617\"[label = \" <348> |348|  <349> |349|  <350> |350|  <351> |351|  <352> |352|  <353> |353| \"];\n" +
                "\"626\"[label = \" <354> |354|  <355> |355|  <356> |356|  <357> |357|  <358> |358|  <359> |359| \"];\n" +
                "\"688\"[label = \" <635> |366|  <652> |372|  <662> |378|  <lastChild> |Ls \"];\n" +
                "\"688\":635 -> \"635\"\n" +
                "\"688\":652 -> \"652\"\n" +
                "\"688\":662 -> \"662\"\n" +
                "\"688\":lastChild -> \"671\"\n" +
                "\"635\"[label = \" <360> |360|  <361> |361|  <362> |362|  <363> |363|  <364> |364|  <365> |365| \"];\n" +
                "\"652\"[label = \" <366> |366|  <367> |367|  <368> |368|  <369> |369|  <370> |370|  <371> |371| \"];\n" +
                "\"662\"[label = \" <372> |372|  <373> |373|  <374> |374|  <375> |375|  <376> |376|  <377> |377| \"];\n" +
                "\"671\"[label = \" <378> |378|  <379> |379|  <380> |380|  <381> |381|  <382> |382|  <383> |383| \"];\n" +
                "\"860\"[label = \"{ log | { 498-498 }}| <733> |408|  <778> |432|  <824> |456|  <lastChild> |Ls \"];\n" +
                "\"860\":733 -> \"733\"\n" +
                "\"860\":778 -> \"778\"\n" +
                "\"860\":824 -> \"824\"\n" +
                "\"860\":lastChild -> \"861\"\n" +
                "\"733\"[label = \" <680> |390|  <689> |396|  <707> |402|  <lastChild> |Ls \"];\n" +
                "\"733\":680 -> \"680\"\n" +
                "\"733\":689 -> \"689\"\n" +
                "\"733\":707 -> \"707\"\n" +
                "\"733\":lastChild -> \"716\"\n" +
                "\"680\"[label = \" <384> |384|  <385> |385|  <386> |386|  <387> |387|  <388> |388|  <389> |389| \"];\n" +
                "\"689\"[label = \" <390> |390|  <391> |391|  <392> |392|  <393> |393|  <394> |394|  <395> |395| \"];\n" +
                "\"707\"[label = \" <396> |396|  <397> |397|  <398> |398|  <399> |399|  <400> |400|  <401> |401| \"];\n" +
                "\"716\"[label = \" <402> |402|  <403> |403|  <404> |404|  <405> |405|  <406> |406|  <407> |407| \"];\n" +
                "\"778\"[label = \" <725> |414|  <734> |420|  <744> |426|  <lastChild> |Ls \"];\n" +
                "\"778\":725 -> \"725\"\n" +
                "\"778\":734 -> \"734\"\n" +
                "\"778\":744 -> \"744\"\n" +
                "\"778\":lastChild -> \"761\"\n" +
                "\"725\"[label = \" <408> |408|  <409> |409|  <410> |410|  <411> |411|  <412> |412|  <413> |413| \"];\n" +
                "\"734\"[label = \" <414> |414|  <415> |415|  <416> |416|  <417> |417|  <418> |418|  <419> |419| \"];\n" +
                "\"744\"[label = \" <420> |420|  <421> |421|  <422> |422|  <423> |423|  <424> |424|  <425> |425| \"];\n" +
                "\"761\"[label = \" <426> |426|  <427> |427|  <428> |428|  <429> |429|  <430> |430|  <431> |431| \"];\n" +
                "\"824\"[label = \" <770> |438|  <779> |444|  <790> |450|  <lastChild> |Ls \"];\n" +
                "\"824\":770 -> \"770\"\n" +
                "\"824\":779 -> \"779\"\n" +
                "\"824\":790 -> \"790\"\n" +
                "\"824\":lastChild -> \"799\"\n" +
                "\"770\"[label = \" <432> |432|  <433> |433|  <434> |434|  <435> |435|  <436> |436|  <437> |437| \"];\n" +
                "\"779\"[label = \" <438> |438|  <439> |439|  <440> |440|  <441> |441|  <442> |442|  <443> |443| \"];\n" +
                "\"790\"[label = \" <444> |444|  <445> |445|  <446> |446|  <447> |447|  <448> |448|  <449> |449| \"];\n" +
                "\"799\"[label = \" <450> |450|  <451> |451|  <452> |452|  <453> |453|  <454> |454|  <455> |455| \"];\n" +
                "\"861\"[label = \"{ log | { 499-499 }}| <816> |462|  <825> |468|  <835> |474|  <844> |480|  <853> |486|  <lastChild> |Ls \"];\n" +
                "\"861\":816 -> \"816\"\n" +
                "\"861\":825 -> \"825\"\n" +
                "\"861\":835 -> \"835\"\n" +
                "\"861\":844 -> \"844\"\n" +
                "\"861\":853 -> \"853\"\n" +
                "\"861\":lastChild -> \"862\"\n" +
                "\"816\"[label = \" <456> |456|  <457> |457|  <458> |458|  <459> |459|  <460> |460|  <461> |461| \"];\n" +
                "\"825\"[label = \" <462> |462|  <463> |463|  <464> |464|  <465> |465|  <466> |466|  <467> |467| \"];\n" +
                "\"835\"[label = \" <468> |468|  <469> |469|  <470> |470|  <471> |471|  <472> |472|  <473> |473| \"];\n" +
                "\"844\"[label = \" <474> |474|  <475> |475|  <476> |476|  <477> |477|  <478> |478|  <479> |479| \"];\n" +
                "\"853\"[label = \" <480> |480|  <481> |481|  <482> |482|  <483> |483|  <484> |484|  <485> |485| \"];\n" +
                "\"862\"[label = \" <486> |486|  <487> |487|  <488> |488|  <489> |489|  <490> |490|  <491> |491|  <492> |492|  <493> |493|  <494> |494|  <495> |495|  <496> |496|  <497> |497| \"];\n" +
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

        assertEquals(5L, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"915\"[label = \" <823> |168|  <lastChild> |Ls \"];\n" +
                "\"915\":823 -> \"823\"\n" +
                "\"915\":lastChild -> \"914\"\n" +
                "\"823\"[label = \"\"];\n" +
                "\"914\"[label = \" <874> |186|  <lastChild> |Ls \"];\n" +
                "\"914\":874 -> \"874\"\n" +
                "\"914\":lastChild -> \"910\"\n" +
                "\"874\"[label = \"\"];\n" +
                "\"910\"[label = \"\"];\n" +
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

        assertEquals(1, bTree.getNodesCount());

        final String expectedTree = "digraph g {\n" +
                "node [shape = record,height=.1];\n" +
                "\"663\"[label = \"\"];\n" +
                "}\n";

        assertEquals(expectedTree, bTree.print());
    }
}