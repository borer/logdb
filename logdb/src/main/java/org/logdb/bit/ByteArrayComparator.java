package org.logdb.bit;

import java.util.Comparator;

public final class ByteArrayComparator implements Comparator<byte[]>
{
    public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

    private ByteArrayComparator()
    {
    }

    @Override
    public int compare(final byte[] a, final byte[] b)
    {
        final int minLength = a.length < b.length ? a.length : b.length;
        for (int i = 0; i < minLength; i++)
        {
            if (a[i] != b[i])
            {
                return Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
            }
        }

        return a.length - b.length;
    }
}
