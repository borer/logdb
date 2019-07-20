package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.support.StubTimeSource;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;
import static org.logdb.integration.TestIntegrationUtils.createNewPersistedBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedBtree;

class BBTreeIntegrationTest
{
    @TempDir
    Path tempDirectory;

    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNode() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedBtree(tempDirectory))
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
    void shouldBeABleToCreateNewDbFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final long nonExistingKeyValuePair = 1919191919L;

        try (final BTree bTree = createNewPersistedBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
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
    void shouldBeABleToPersistAndReadABBtree() throws Exception
    {
        final int numKeys = 100;

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (int i = 0; i < numKeys; i++)
            {
                originalBTree.put(i, i);
            }

            originalBTree.commit();
        }

        try (final BTree loadedBTree = loadPersistedBtree(tempDirectory))
        {
            final String loadedStructure = loadedBTree.print();
            final String expectedStructure = "digraph g {\n" +
                    "node [shape = record,height=.1];\n" +
                    "\"179\"[label = \" <28> |14|  <57> |28|  <86> |42|  <115> |56|  <144> |70|  <173> |84|  <lastChild> |Ls \"];\n" +
                    "\"179\":28 -> \"28\"\n" +
                    "\"179\":57 -> \"57\"\n" +
                    "\"179\":86 -> \"86\"\n" +
                    "\"179\":115 -> \"115\"\n" +
                    "\"179\":144 -> \"144\"\n" +
                    "\"179\":173 -> \"173\"\n" +
                    "\"179\":lastChild -> \"178\"\n" +
                    "\"28\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9|  <10> |10|  <11> |11|  <12> |12|  <13> |13| \"];\n" +
                    "\"57\"[label = \" <14> |14|  <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19|  <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24|  <25> |25|  <26> |26|  <27> |27| \"];\n" +
                    "\"86\"[label = \" <28> |28|  <29> |29|  <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39|  <40> |40|  <41> |41| \"];\n" +
                    "\"115\"[label = \" <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49|  <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54|  <55> |55| \"];\n" +
                    "\"144\"[label = \" <56> |56|  <57> |57|  <58> |58|  <59> |59|  <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                    "\"173\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79|  <80> |80|  <81> |81|  <82> |82|  <83> |83| \"];\n" +
                    "\"178\"[label = \" <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89|  <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                    "}\n";

            assertEquals(expectedStructure, loadedStructure);

            for (int i = 0; i < numKeys; i++)
            {
                assertEquals(i, loadedBTree.get(i));
            }
        }
    }

    @Test
    void shouldBeABleToCommitMultipleTimes() throws Exception
    {
        final int numberOfPairs = 100;
        final List<Long> expectedOrder = new ArrayList<>();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairs; i++)
            {
                expectedOrder.add(i);
                originalBTree.put(i, i);
                originalBTree.commit();
            }

            originalBTree.commit();
        }

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {

            loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

            assertEquals(expectedOrder.size(), actualOrder.size());

            for (int i = 0; i < expectedOrder.size(); i++)
            {
                assertEquals(expectedOrder.get(i), actualOrder.get(i));
            }
        }
    }

    @Test
    void shouldConsumeKeyValuesInOrderAfterCommit() throws Exception
    {
        final List<Long> expectedOrder = new ArrayList<>();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < 100; i++)
            {
                expectedOrder.add(i);
                originalBTree.put(i, i);
            }

            originalBTree.commit();
        }

        expectedOrder.sort(Long::compareTo);

        final LinkedList<Long> actualOrder = new LinkedList<>();
        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

            assertEquals(expectedOrder.size(), actualOrder.size());

            for (int i = 0; i < expectedOrder.size(); i++)
            {
                assertEquals(expectedOrder.get(i), actualOrder.get(i));
            }
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDB() throws Exception
    {
        final long key = 123L;
        final int maxVersions = 100;

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
            }

            originalBTree.commit();
        }

        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            final int[] index = new int[1]; //ugh... lambdas
            for (index[0] = 0; index[0] < maxVersions; index[0]++)
            {
                loadedBTree.consumeAll(index[0], (k, value) ->
                {
                    assertEquals(key, k);
                    assertEquals(index[0], value);
                });
            }
        }
    }

    @Test
    void shouldGetHistoricValuesByTimestampFromOpenDB() throws Exception
    {
        final long key = 123L;
        final int maxVersions = 100;

        final StubTimeSource timeSource = new StubTimeSource();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory, timeSource))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                originalBTree.put(key, i);
            }

            originalBTree.commit();
        }

        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            for (int i = 0; i < timeSource.getCurrentTimeWithoutIncrementing(); i++)
            {
                assertEquals(i, loadedBTree.getByTimestamp(key, i));
            }
        }
    }
}
