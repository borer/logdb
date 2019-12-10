package org.logdb.checksum;

class BinaryHelper
{
    static void longToBytes(final long value, final byte[] bytes)
    {
        longToBytes(value, bytes, 0);
    }

    static void longToBytes(final long value, final byte[] bytes, final int offset)
    {
        bytes[offset] = (byte)value;
        bytes[offset + 1] = (byte)(value >> Byte.SIZE);
        bytes[offset + 2] = (byte)(value >> 16);
        bytes[offset + 3] = (byte)(value >> 24);
        bytes[offset + 4] = (byte)(value >> 32);
        bytes[offset + 5] = (byte)(value >> 40);
        bytes[offset + 6] = (byte)(value >> 48);
        bytes[offset + 7] = (byte)(value >> 56);
    }

    static long bytesToLong(final byte[] bytes)
    {
        return ((long) bytes[7] << 56) |
                ((long) bytes[6] & 0xff) << 48 |
                ((long) bytes[5] & 0xff) << 40 |
                ((long) bytes[4] & 0xff) << 32 |
                ((long) bytes[3] & 0xff) << 24 |
                ((long) bytes[2] & 0xff) << 16 |
                ((long) bytes[1] & 0xff) << 8 |
                ((long) bytes[0] & 0xff);
    }

    static int bytesToInt(final byte[] bytes)
    {
        return ((int) bytes[3] & 0xff) << 24 |
                ((int) bytes[2] & 0xff) << 16 |
                ((int) bytes[1] & 0xff) << 8 |
                ((int) bytes[0] & 0xff);
    }

    static void intToBytes(final int value, final byte[] bytes)
    {
        bytes[0] = (byte)value;
        bytes[1] = (byte)(value >> Byte.SIZE);
        bytes[2] = (byte)(value >> 16);
        bytes[3] = (byte)(value >> 24);
    }
}
