package org.logdb.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.bit.BinaryHelper;
import org.logdb.bit.ByteArrayComparator;
import org.logdb.support.StubTimeSource;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.integration.TestIntegrationUtils.createNewPersistedBtree;
import static org.logdb.integration.TestIntegrationUtils.loadPersistedBtree;

class BBTreeIntegrationTest
{
    @TempDir
    Path tempDirectory;

    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNode() throws Exception
    {
        final byte[] nonExistingKeyValuePair = BinaryHelper.longToBytes(1919191919L);
        final byte[] bytesOne = BinaryHelper.longToBytes(1);
        final byte[] bytesFive = BinaryHelper.longToBytes(5);
        final byte[] bytesTen = BinaryHelper.longToBytes(10);

        try (final BTree bTree = createNewPersistedBtree(tempDirectory))
        {
            bTree.put(bytesOne, bytesOne);
            bTree.put(bytesTen, bytesTen);
            bTree.put(bytesFive, bytesFive);

            assertArrayEquals(null, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(tempDirectory))
        {
            assertArrayEquals(bytesOne, readBTree.get(bytesOne));
            assertArrayEquals(bytesTen, readBTree.get(bytesTen));
            assertArrayEquals(bytesFive, readBTree.get(bytesFive));

            assertArrayEquals(null, readBTree.get(nonExistingKeyValuePair));
        }
    }

    @Test
    void shouldBeABleToCreateNewDbFileAndReadLeafNodeWithBigEndianEncoding() throws Exception
    {
        final byte[] nonExistingKeyValuePair = BinaryHelper.longToBytes(1919191919L);
        final byte[] bytesOne = BinaryHelper.longToBytes(1);
        final byte[] bytesFive = BinaryHelper.longToBytes(5);
        final byte[] bytesTen = BinaryHelper.longToBytes(10);

        try (final BTree bTree = createNewPersistedBtree(tempDirectory, ByteOrder.BIG_ENDIAN))
        {
            bTree.put(bytesOne, bytesOne);
            bTree.put(bytesTen, bytesTen);
            bTree.put(bytesFive, bytesFive);

            assertArrayEquals(null, bTree.get(nonExistingKeyValuePair));

            bTree.commit();
        }

        try (final BTree readBTree = loadPersistedBtree(tempDirectory))
        {
            assertArrayEquals(bytesOne, readBTree.get(bytesOne));
            assertArrayEquals(bytesTen, readBTree.get(bytesTen));
            assertArrayEquals(bytesFive, readBTree.get(bytesFive));

            assertArrayEquals(null, readBTree.get(nonExistingKeyValuePair));
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
                final byte[] bytes = BinaryHelper.longToBytes(i);
                originalBTree.put(bytes, bytes);
            }

            originalBTree.commit();
        }

        try (final BTree loadedBTree = loadPersistedBtree(tempDirectory))
        {
            final String loadedStructure = loadedBTree.print();
            final String expectedStructure = "digraph g {\n" +
                    "node [shape = record,height=.1];\n" +
                    "\"189\"[label = \" <24> |10|  <45> |20|  <66> |30|  <87> |40|  <108> |50|  <129> |60|  <150> |70|  <171> |80|  <lastChild> |Ls \"];\n" +
                    "\"189\":24 -> \"24\"\n" +
                    "\"189\":45 -> \"45\"\n" +
                    "\"189\":66 -> \"66\"\n" +
                    "\"189\":87 -> \"87\"\n" +
                    "\"189\":108 -> \"108\"\n" +
                    "\"189\":129 -> \"129\"\n" +
                    "\"189\":150 -> \"150\"\n" +
                    "\"189\":171 -> \"171\"\n" +
                    "\"189\":lastChild -> \"188\"\n" +
                    "\"24\"[label = \" <0> |0|  <1> |1|  <2> |2|  <3> |3|  <4> |4|  <5> |5|  <6> |6|  <7> |7|  <8> |8|  <9> |9| \"];\n" +
                    "\"45\"[label = \" <10> |10|  <11> |11|  <12> |12|  <13> |13|  <14> |14|  <15> |15|  <16> |16|  <17> |17|  <18> |18|  <19> |19| \"];\n" +
                    "\"66\"[label = \" <20> |20|  <21> |21|  <22> |22|  <23> |23|  <24> |24|  <25> |25|  <26> |26|  <27> |27|  <28> |28|  <29> |29| \"];\n" +
                    "\"87\"[label = \" <30> |30|  <31> |31|  <32> |32|  <33> |33|  <34> |34|  <35> |35|  <36> |36|  <37> |37|  <38> |38|  <39> |39| \"];\n" +
                    "\"108\"[label = \" <40> |40|  <41> |41|  <42> |42|  <43> |43|  <44> |44|  <45> |45|  <46> |46|  <47> |47|  <48> |48|  <49> |49| \"];\n" +
                    "\"129\"[label = \" <50> |50|  <51> |51|  <52> |52|  <53> |53|  <54> |54|  <55> |55|  <56> |56|  <57> |57|  <58> |58|  <59> |59| \"];\n" +
                    "\"150\"[label = \" <60> |60|  <61> |61|  <62> |62|  <63> |63|  <64> |64|  <65> |65|  <66> |66|  <67> |67|  <68> |68|  <69> |69| \"];\n" +
                    "\"171\"[label = \" <70> |70|  <71> |71|  <72> |72|  <73> |73|  <74> |74|  <75> |75|  <76> |76|  <77> |77|  <78> |78|  <79> |79| \"];\n" +
                    "\"188\"[label = \" <80> |80|  <81> |81|  <82> |82|  <83> |83|  <84> |84|  <85> |85|  <86> |86|  <87> |87|  <88> |88|  <89> |89|  <90> |90|  <91> |91|  <92> |92|  <93> |93|  <94> |94|  <95> |95|  <96> |96|  <97> |97|  <98> |98|  <99> |99| \"];\n" +
                    "}\n";

            assertEquals(expectedStructure, loadedStructure);

            for (int i = 0; i < numKeys; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                assertArrayEquals(bytes, loadedBTree.get(bytes));
            }
        }
    }

    @Test
    void shouldBeABleToCommitMultipleTimes() throws Exception
    {
        final int numberOfPairs = 100;
        final List<byte[]> expectedOrder = new ArrayList<>();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < numberOfPairs; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);

                expectedOrder.add(bytes);
                originalBTree.put(bytes, bytes);
                originalBTree.commit();
            }

            originalBTree.commit();
        }

        expectedOrder.sort(ByteArrayComparator.INSTANCE);

        final LinkedList<byte[]> actualOrder = new LinkedList<>();
        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

            assertEquals(expectedOrder.size(), actualOrder.size());

            for (int i = 0; i < expectedOrder.size(); i++)
            {
                assertArrayEquals(expectedOrder.get(i), actualOrder.get(i));
            }
        }
    }

    @Test
    void shouldConsumeKeyValuesInOrderAfterCommit() throws Exception
    {
        final List<byte[]> expectedOrder = new ArrayList<>();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < 100; i++)
            {
                final byte[] bytes = BinaryHelper.longToBytes(i);
                expectedOrder.add(bytes);
                originalBTree.put(bytes, bytes);
            }

            originalBTree.commit();
        }

        expectedOrder.sort(ByteArrayComparator.INSTANCE);

        final LinkedList<byte[]> actualOrder = new LinkedList<>();
        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            loadedBTree.consumeAll((key, value) -> actualOrder.addLast(key));

            assertEquals(expectedOrder.size(), actualOrder.size());

            for (int i = 0; i < expectedOrder.size(); i++)
            {
                assertArrayEquals(expectedOrder.get(i), actualOrder.get(i));
            }
        }
    }

    @Test
    void shouldGetHistoricValuesFromOpenDB() throws Exception
    {
        final byte[] key = BinaryHelper.longToBytes(123L);
        final int maxVersions = 100;

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                originalBTree.put(key, value);
            }

            originalBTree.commit();
        }

        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            final int[] version = new int[1]; //ugh... lambdas
            for (version[0] = 0; version[0] < maxVersions; version[0]++)
            {
                loadedBTree.consumeAll(version[0], (k, value) ->
                {
                    final byte[] expectedValue = BinaryHelper.longToBytes(version[0]);
                    assertArrayEquals(key, k);
                    assertArrayEquals(expectedValue, value);
                });
            }
        }
    }

    @Test
    void shouldGetHistoricValuesByTimestampFromOpenDB() throws Exception
    {
        final byte[] key = BinaryHelper.longToBytes(123L);
        final int maxVersions = 100;

        final StubTimeSource timeSource = new StubTimeSource();

        try (final BTree originalBTree = createNewPersistedBtree(tempDirectory, timeSource))
        {
            for (long i = 0; i < maxVersions; i++)
            {
                final byte[] value = BinaryHelper.longToBytes(i);
                originalBTree.put(key, value);
            }

            originalBTree.commit();
        }

        try (final BTreeImpl loadedBTree = loadPersistedBtree(tempDirectory))
        {
            for (int i = 0; i < timeSource.getCurrentTimeWithoutIncrementing(); i++)
            {
                final byte[] expectedValue = BinaryHelper.longToBytes(i);
                assertArrayEquals(expectedValue, loadedBTree.getByTimestamp(key, i));
            }
        }
    }
}
