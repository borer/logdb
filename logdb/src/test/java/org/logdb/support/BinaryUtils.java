package org.logdb.support;

public class BinaryUtils
{
    public static long bytesToLong(final byte[] bytes)
    {
        if (bytes.length != Long.BYTES)
        {
            throw new ArrayIndexOutOfBoundsException("Required an array of " + Long.BYTES + " bytes. Got " + bytes.length);
        }

        return ((long) bytes[7] << 56)
                | ((long) bytes[6] & 0xff) << 48
                | ((long) bytes[5] & 0xff) << 40
                | ((long) bytes[4] & 0xff) << 32
                | ((long) bytes[3] & 0xff) << 24
                | ((long) bytes[2] & 0xff) << 16
                | ((long) bytes[1] & 0xff) << 8
                | ((long) bytes[0] & 0xff);
    }
}
