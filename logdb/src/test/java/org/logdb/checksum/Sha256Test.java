package org.logdb.checksum;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class Sha256Test
{
    private Sha256 sha256;

    @BeforeEach
    void setUp() throws NoSuchAlgorithmException
    {
        sha256 = new Sha256();
    }

    @Test
    void shouldCalculateEmptyChecksum()
    {
        final byte[] expectedBytes = {-29,-80,-60,66,-104,-4,28,20,-102,-5,-12,-56,-103,111,-71,36,39,-82,65,-28,100,-101,-109,76,-92,-107,-103,27,120,82,-72,85};
        assertArrayEquals(expectedBytes, sha256.getValue());
    }

    @Test
    void shouldCalculateChecksumAfterReset()
    {
        final byte[] expectedBytes = {-29,-80,-60,66,-104,-4,28,20,-102,-5,-12,-56,-103,111,-71,36,39,-82,65,-28,100,-101,-109,76,-92,-107,-103,27,120,82,-72,85};
        sha256.reset();
        assertArrayEquals(expectedBytes, sha256.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksum()
    {
        final byte[] expectedBytes = {21,-30,-80,-45,-61,56,-111,-21,-80,-15,-17,96,-98,-60,25,66,12,32,-29,32,-50,-108,-58,95,-68,-116,51,18,68,-114,-78,37};
        sha256.update("123456789".getBytes());
        assertArrayEquals(expectedBytes, sha256.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumAfterMultipleUpdates()
    {
        final byte[] expectedBytes = {95,-2,-46,-31,-67,86,-77,126,58,-105,74,38,-102,28,38,-87,-81,-121,-10,-123,76,-90,4,72,71,58,114,-110,86,-18,4,15};
        sha256.update("123456789".getBytes());
        sha256.update("987654321".getBytes());
        assertArrayEquals(expectedBytes, sha256.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumAfterReset()
    {
        final byte[] expectedBytes = {21,-30,-80,-45,-61,56,-111,-21,-80,-15,-17,96,-98,-60,25,66,12,32,-29,32,-50,-108,-58,95,-68,-116,51,18,68,-114,-78,37};
        final byte[] moreExpectedBytes = {-118,-101,-49,30,81,-24,18,-48,-81,-124,101,-88,-37,-52,-97,116,16,100,-65,10,-13,-77,-48,-114,107,2,70,67,124,25,-9,-5};

        sha256.update("123456789".getBytes());
        assertArrayEquals(expectedBytes, sha256.getValue());

        sha256.reset();
        sha256.update("987654321".getBytes());
        assertArrayEquals(moreExpectedBytes, sha256.getValue());
    }

    @Test
    void shouldCalculateCorrectChecksumForLongMessage()
    {
        final byte[] expectedBytes = {29,41,-93,53,100,-109,-23,-69,24,55,-30,-2,-27,-14,118,-39,-125,91,14,47,-52,95,29,49,-39,-10,-53,-5,-40,38,16,19};

        sha256.update("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Id porta nibh venenatis cras sed felis eget velit aliquet. Dictumst quisque sagittis purus sit amet volutpat consequat mauris nunc. Nec feugiat nisl pretium fusce id velit ut tortor pretium. Nulla aliquet porttitor lacus luctus accumsan tortor. Nec feugiat in fermentum posuere urna nec tincidunt. Odio tempor orci dapibus ultrices in iaculis nunc sed. Gravida quis blandit turpis cursus in hac habitasse platea. Urna id volutpat lacus laoreet non curabitur gravida arcu ac. A erat nam at lectus urna duis convallis convallis. Eros donec ac odio tempor orci dapibus ultrices in iaculis. Risus feugiat in ante metus dictum at. Molestie at elementum eu facilisis sed odio. Consequat nisl vel pretium lectus quam id leo in.".getBytes());
        assertArrayEquals(expectedBytes, sha256.getValue());
    }
}