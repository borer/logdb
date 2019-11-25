package org.logdb.checksum;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.checksum.BinaryHelper.bytesToLong;

class Crc32Test
{
    private Crc32 crc32;

    @BeforeEach
    void setUp()
    {
        crc32 = new Crc32();
    }

    @Test
    void shouldCalculateEmptyChecksum()
    {
        assertEquals(0, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldCalculateChecksumAfterReset()
    {
        crc32.reset();
        assertEquals(0, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksum()
    {
        crc32.update("123456789".getBytes());
        assertEquals(3421780262L, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumAfterMultipleUpdates()
    {
        crc32.update("123456789".getBytes());
        assertEquals(3421780262L, bytesToLong(crc32.getValue()));

        crc32.update("987654321".getBytes());
        assertEquals(2166899139L, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumAfterReset()
    {
        crc32.update("123456789".getBytes());
        assertEquals(3421780262L, bytesToLong(crc32.getValue()));

        crc32.reset();
        crc32.update("987654321".getBytes());
        assertEquals(23003649, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumForLongMessage()
    {
        crc32.update("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Id porta nibh venenatis cras sed felis eget velit aliquet. Dictumst quisque sagittis purus sit amet volutpat consequat mauris nunc. Nec feugiat nisl pretium fusce id velit ut tortor pretium. Nulla aliquet porttitor lacus luctus accumsan tortor. Nec feugiat in fermentum posuere urna nec tincidunt. Odio tempor orci dapibus ultrices in iaculis nunc sed. Gravida quis blandit turpis cursus in hac habitasse platea. Urna id volutpat lacus laoreet non curabitur gravida arcu ac. A erat nam at lectus urna duis convallis convallis. Eros donec ac odio tempor orci dapibus ultrices in iaculis. Risus feugiat in ante metus dictum at. Molestie at elementum eu facilisis sed odio. Consequat nisl vel pretium lectus quam id leo in.".getBytes());
        assertEquals(1892177047L, bytesToLong(crc32.getValue()));
    }

    @Test
    void shouldBeAbleToGenerateValidTable()
    {
        final int[] generatedTable = Crc32.generateTable();

        assertEquals(Crc32.LOOKUP_TABLE.length, generatedTable.length);

        for (int i = 0; i < Crc32.LOOKUP_TABLE.length; i++)
        {
            assertEquals(Crc32.LOOKUP_TABLE[i], generatedTable[i]);
        }
    }
}