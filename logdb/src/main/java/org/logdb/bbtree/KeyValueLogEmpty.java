package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteSize;

import static org.logdb.storage.StorageUnits.ZERO_SIZE;

public final class KeyValueLogEmpty implements KeyValueLog
{
    public static final KeyValueLogEmpty INSTANCE = new KeyValueLogEmpty();

    private KeyValueLogEmpty()
    {

    }

    @Override
    public void resetLog()
    {

    }

    @Override
    public int getNumberOfPairs()
    {
        return 0;
    }

    @Override
    public boolean removeLogBytes(byte[] key)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to remove %s", new String(key)));
    }

    @Override
    public @ByteSize long getUsedSize()
    {
        return ZERO_SIZE;
    }

    @Override
    public void cacheNumberOfLogPairs()
    {

    }

    @Override
    public int binarySearchInLog(byte[] key)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to search %s", new String(key)));
    }

    @Override
    public byte[] getKeyBytesAtIndex(int index)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public byte[] getValueBytesAtIndex(int index)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void putKeyValue(int i, byte[] keyBytes, byte[] valueBytes)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to put %s, value %s", new String(keyBytes), new String(valueBytes)));
    }

    @Override
    public void insertLog(byte[] key, byte[] value)
    {
        throw new RuntimeException(
                String.format("Method not implemented. value to set %s", new String(value)));
    }

    @Override
    public KeyValueLogImpl spillLog()
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void splitLog(byte[] key, KeyValueLog keyValueLog)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public Memory getMemory()
    {
        throw new RuntimeException("Method not implemented.");
    }
}
