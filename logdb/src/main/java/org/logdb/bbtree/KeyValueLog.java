package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteSize;

public interface KeyValueLog
{
    void resetLog();

    int getNumberOfPairs();

    boolean removeLogBytes(byte[] key);

    @ByteSize long getUsedSize();

    void cacheNumberOfLogPairs();

    int binarySearchInLog(byte[] key);

    byte[] getKeyBytesAtIndex(int index);

    byte[] getValueBytesAtIndex(int index);

    void insertLog(byte[] key, byte[] value);

    void putKeyValue(int index, byte[] key, byte[] value);

    KeyValueLogImpl spillLog();

    void splitLog(byte[] key, KeyValueLog keyValueLog);

    Memory getMemory();
}
