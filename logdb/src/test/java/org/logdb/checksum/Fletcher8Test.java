package org.logdb.checksum;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.logdb.checksum.BinaryHelper.bytesToLong;

class Fletcher8Test
{
    private Fletcher8 fletcher8;

    @BeforeEach
    void setUp()
    {
        fletcher8 = new Fletcher8();
    }

    @Test
    void shouldCalculateEmptyChecksum()
    {
        assertEquals(0, bytesToLong(fletcher8.getValue()));
    }

    @Test
    void shouldCalculateChecksumAfterReset()
    {
        fletcher8.reset();
        assertEquals(0, bytesToLong(fletcher8.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksum()
    {
        fletcher8.update("123456789".getBytes());
        assertEquals(-1152974287884359521L, bytesToLong(fletcher8.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumAfterMultipleUpdates()
    {
        fletcher8.update("123456789".getBytes());
        assertEquals(-1152974287884359521L, bytesToLong(fletcher8.getValue()));

        fletcher8.update("987654321".getBytes());
        assertEquals(-555887298L, bytesToLong(fletcher8.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumAfterReset()
    {
        fletcher8.update("123456789".getBytes());
        assertEquals(-1152974287884359521L, bytesToLong(fletcher8.getValue()));

        fletcher8.reset();
        fletcher8.update("987654321".getBytes());
        assertEquals(-2594093011328668513L, bytesToLong(fletcher8.getValue()));
    }

    @Test
    void shouldCalculateCorrectChecksumForLongMessage()
    {
        fletcher8.update("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Id porta nibh venenatis cras sed felis eget velit aliquet. Dictumst quisque sagittis purus sit amet volutpat consequat mauris nunc. Nec feugiat nisl pretium fusce id velit ut tortor pretium. Nulla aliquet porttitor lacus luctus accumsan tortor. Nec feugiat in fermentum posuere urna nec tincidunt. Odio tempor orci dapibus ultrices in iaculis nunc sed. Gravida quis blandit turpis cursus in hac habitasse platea. Urna id volutpat lacus laoreet non curabitur gravida arcu ac. A erat nam at lectus urna duis convallis convallis. Eros donec ac odio tempor orci dapibus ultrices in iaculis. Risus feugiat in ante metus dictum at. Molestie at elementum eu facilisis sed odio. Consequat nisl vel pretium lectus quam id leo in.".getBytes());
        assertEquals(-1098996506L, bytesToLong(fletcher8.getValue()));
    }
}