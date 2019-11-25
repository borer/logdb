package org.logdb.checksum;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Sha256 implements Checksum
{
    private final MessageDigest instance;

    public Sha256() throws NoSuchAlgorithmException
    {
        instance = MessageDigest.getInstance("SHA-256");
    }

    @Override
    public void update(final int b)
    {
        instance.update((byte)b);
    }

    public void update(final byte[] bytes)
    {
        update(bytes, 0, bytes.length);
    }

    @Override
    public void update(final byte[] bytes, final int offset, final int length)
    {
        instance.update(bytes, offset, length);
    }

    @Override
    public byte[] getValue()
    {
        return instance.digest();
    }

    @Override
    public void reset()
    {
        instance.reset();
    }

    @Override
    public @ByteSize int getValueSize()
    {
        return StorageUnits.size(32);
    }

    @Override
    public boolean compare(final byte[] bytes)
    {
        final byte[] currectValue = getValue();

        return Arrays.equals(currectValue, bytes);
    }
}
