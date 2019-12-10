package org.logdb.checksum;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.checksum.BinaryHelper.bytesToLong;

class Fletcher32Test
{
    private Fletcher32 fletcher32;

    @BeforeEach
    void setUp()
    {
        fletcher32 = new Fletcher32();
    }

    @Test
    void shouldCalculateEmptyChecksum()
    {
        assertEquals(0, bytesToLong(fletcher32.getValue()));
    }

    @Test
    void shouldCalculateChecksumAfterReset()
    {
        fletcher32.reset();
        assertEquals(0, bytesToLong(fletcher32.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksum()
    {
        final byte[] expect = new byte[]{-97, 104, 106, 108, 0, 0, 0, 0, 54, 3, 8, 13, 0, 0, 0, 0, -2, -49, -40, -31, -1, -1, -1, -1, -9, -50, -36, -22, -1, -1, -1, -1};
        fletcher32.update("123456789".getBytes());
        assertArrayEquals(expect, fletcher32.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumAfterMultipleUpdates()
    {
        final byte[] expect = new byte[]{-97, 104, 106, 108, 0, 0, 0, 0, 54, 3, 8, 13, 0, 0, 0, 0, -2, -49, -40, -31, -1, -1, -1, -1, -9, -50, -36, -22, -1, -1, -1, -1};
        fletcher32.update("123456789".getBytes());
        assertArrayEquals(expect, fletcher32.getValue());

        fletcher32.update("987654321".getBytes());
        final byte[] expected2 = new byte[] {62, -43, -44, -44, -1, -1, -1, -1, 89, 78, 83, 89, 0, 0, 0, 0, -128, 59, 84, 111, 0, 0, 0, 0, -86, 107, -76, 1, 0, 0, 0, 0};
        assertArrayEquals(expected2, fletcher32.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumAfterReset()
    {
        fletcher32.update("123456789".getBytes());
        final byte[] expect = new byte[]{-97, 104, 106, 108, 0, 0, 0, 0, 54, 3, 8, 13, 0, 0, 0, 0, -2, -49, -40, -31, -1, -1, -1, -1, -9, -50, -36, -22, -1, -1, -1, -1};
        assertArrayEquals(expect, fletcher32.getValue());

        fletcher32.reset();
        fletcher32.update("987654321".getBytes());
        final byte[] expect2 = new byte[]{-97, 108, 106, 104, 0, 0, 0, 0, 70, 17, 12, 7, 0, 0, 0, 0, 38, -18, -28, -37, -1, -1, -1, -1, 63, 3, -11, -26, -1, -1, -1, -1};
        assertArrayEquals(expect2, fletcher32.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumForLongMessage()
    {
        fletcher32.update("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Id porta nibh venenatis cras sed felis eget velit aliquet. Dictumst quisque sagittis purus sit amet volutpat consequat mauris nunc. Nec feugiat nisl pretium fusce id velit ut tortor pretium. Nulla aliquet porttitor lacus luctus accumsan tortor. Nec feugiat in fermentum posuere urna nec tincidunt. Odio tempor orci dapibus ultrices in iaculis nunc sed. Gravida quis blandit turpis cursus in hac habitasse platea. Urna id volutpat lacus laoreet non curabitur gravida arcu ac. A erat nam at lectus urna duis convallis convallis. Eros donec ac odio tempor orci dapibus ultrices in iaculis. Risus feugiat in ante metus dictum at. Molestie at elementum eu facilisis sed odio. Consequat nisl vel pretium lectus quam id leo in.".getBytes());
        final byte[] expect = new byte[]{-26, -92, 126, 34, 0, 0, 0, 0, 62, -100, 96, 97, 0, 0, 0, 0, -115, -85, -72, 122, 0, 0, 0, 0, -121, -55, -61, 79, 0, 0, 0, 0};
        assertArrayEquals(expect, fletcher32.getValue());
    }
}