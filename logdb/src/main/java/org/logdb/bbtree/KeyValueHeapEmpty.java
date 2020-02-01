package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteSize;

import static org.logdb.storage.StorageUnits.ZERO_SIZE;

public final class KeyValueHeapEmpty implements KeyValueHeap
{
    public static final KeyValueHeapEmpty INSTANCE = new KeyValueHeapEmpty();

    private KeyValueHeapEmpty()
    {

    }

    @Override
    public void reset(final short numberOfEntries)
    {

    }

    @Override
    public int getNumberOfPairs()
    {
        return 0;
    }

    @Override
    public boolean removeKeyValue(byte[] key)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to remove %s", new String(key)));
    }

    @Override
    public boolean removeKeyValueAtIndex(final int index)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key index to remove %d", index));
    }

    @Override
    public void removeOnlyKey(final int index)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key index to remove %d", index));
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
    public int binarySearch(byte[] key)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to search %s", new String(key)));
    }

    @Override
    public byte[] getKeyAtIndex(int index)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public byte[] getValueAtIndex(int index)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void insertAtIndex(int i, byte[] keyBytes, byte[] valueBytes)
    {
        throw new RuntimeException(
                String.format("Method not implemented. Key to put %s, value %s", new String(keyBytes), new String(valueBytes)));
    }

    @Override
    public void setValue(int index, byte[] value)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void putValue(int index, byte[] value)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void insert(byte[] key, byte[] value)
    {
        throw new RuntimeException(
                String.format("Method not implemented. value to set %s", new String(value)));
    }

    @Override
    public KeyValueHeapImpl spill()
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void split(byte[] key, KeyValueHeap newKeyValueHeap)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public void split(final int index, final int newLogKeyValues, final KeyValueHeap newKeyValueHeap)
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public Memory getMemory()
    {
        throw new RuntimeException("Method not implemented.");
    }

    @Override
    public String printDebug()
    {
        return "Empty";
    }
}
