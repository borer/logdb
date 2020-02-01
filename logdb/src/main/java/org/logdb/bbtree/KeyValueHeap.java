package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteSize;

public interface KeyValueHeap
{
    void reset(short numberOfEntries);

    int getNumberOfPairs();

    boolean removeKeyValue(byte[] key);

    boolean removeKeyValueAtIndex(int index);

    void removeOnlyKey(int index);

    @ByteSize long getUsedSize();

    void cacheNumberOfLogPairs();

    int binarySearch(byte[] key);

    byte[] getKeyAtIndex(int index);

    byte[] getValueAtIndex(int index);

    void insert(byte[] key, byte[] value);

    void insertAtIndex(int index, byte[] key, byte[] value);

    void setValue(int index, byte[] value);

    void putValue(int index, byte[] value);

    KeyValueHeapImpl spill();

    void split(byte[] key, KeyValueHeap newKeyValueHeap);

    void split(int index, int newKeyValues, KeyValueHeap newKeyValueHeap);

    Memory getMemory();

    String printDebug();
}
