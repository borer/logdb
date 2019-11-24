package org.logdb.checksum;

public class Fletcher64 implements Checksum
{
    private int a, b, c, d;

    @Override
    public void update(final int byteValue)
    {
        a += byteValue;
        b += a;
        c += b;
        d += c;
    }

    public void update(final byte[] bytes)
    {
        update(bytes, 0, bytes.length);
    }

    @Override
    public void update(final byte[] bytes, final int offset, final int length)
    {
        final int chunkSize = Integer.BYTES;
        final int chunks = (length - offset) / chunkSize;
        for (int i = 0; i < chunks; i++)
        {
            final int chunkOffset = i * chunkSize;
            final int chunk = (bytes[chunkOffset + 3] & 0xff) << 24 |
                            (bytes[chunkOffset + 2] & 0xff) << 16 |
                            (bytes[chunkOffset + 1] & 0xff) << 8 |
                            (bytes[chunkOffset] & 0xff);
            update(chunk);
        }

        for (int i = chunks * chunkSize; i < length; i++)
        {
            update((int)bytes[i] & 0xff);
        }
    }

    @Override
    public byte[] getValue()
    {
        final long value = (long) d << 48 | (long) c << 32 | b << 16 | a;

        final byte[] bytes = new byte[8];
        ChecksumUtil.longToBytes(value, bytes);

        return bytes;
    }

    @Override
    public void reset()
    {
        a = b = c = d = 0;
    }
}
