package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

class KeyValueLog
{
    private final Memory keyValuesBuffer;

    KeyValueLog(final Memory keyValuesBuffer)
    {
        this.keyValuesBuffer = keyValuesBuffer;
    }

    long getNumberOfElements()
    {
        return keyValuesBuffer.getCapacity() / Long.BYTES;
    }

    long getNumberOfPairs()
    {
        return getNumberOfElements() / 2;
    }

    long getKey(final int index)
    {
        return keyValuesBuffer.getLong(getLogKeyIndexOffset(index));
    }

    long getValue(final int index)
    {
        return keyValuesBuffer.getLong(getLogValueIndexOffset(index));
    }

    void putKeyValue(int index, long key, long value)
    {
        keyValuesBuffer.putLong(getLogKeyIndexOffset(index), key);
        keyValuesBuffer.putLong(getLogValueIndexOffset(index), value);
    }

    @ByteOffset long getLogKeyIndexOffset(final int index)
    {
        final @ByteSize long keyValuePairSize = StorageUnits.size(BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        return StorageUnits.offset(keyValuesBuffer.getCapacity() - ((index + 1) * keyValuePairSize));
    }

    @ByteOffset long getLogValueIndexOffset(final int index)
    {
        return StorageUnits.offset(getLogKeyIndexOffset(index) + BTreeNodePage.KEY_SIZE);
    }
}
