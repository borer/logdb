package org.logdb.checksum;

import java.security.NoSuchAlgorithmException;

public class ChecksumFactory
{
    //TODO: add test to make sure behaviour doesn't change by changing the switch numbers
    public static Checksum checksumFromType(final ChecksumType checksumType)
    {
        switch (checksumType)
        {
            case CRC32:
                return new Crc32();
            case FLETCHER8:
                return new Fletcher8();
            case FLETCHER32:
                return new Fletcher32();
            case SHA256:
                try
                {
                    return new Sha256();
                }
                catch (NoSuchAlgorithmException e)
                {
                    throw new RuntimeException("Couldn't find sha256 algorithm for checksum calculation", e);
                }
            default:
                throw new IllegalArgumentException("Unknown checksum type. Provided " + checksumType);
        }
    }
}
