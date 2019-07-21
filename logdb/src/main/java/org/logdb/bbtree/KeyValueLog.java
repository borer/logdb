package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

final class KeyValueLog
{
    private KeyValueLog()
    {
    }

    static long getNumberOfElements(final @ByteSize long capacity)
    {
        return capacity / Long.BYTES;
    }

    static long getNumberOfPairs(final @ByteSize long capacity)
    {
        return getNumberOfElements(capacity) / 2;
    }

    static long getKey(final Memory buffer, final int index)
    {
        return buffer.getLong(getLogKeyIndexOffset(buffer.getCapacity(), index));
    }

    static long getValue(final Memory buffer, final int index)
    {
        return buffer.getLong(getLogValueIndexOffset(buffer.getCapacity(), index));
    }

    static @ByteOffset long getLogKeyIndexOffset(final @ByteSize long pageSize, final int index)
    {
        final @ByteSize long keyValuePairSize = StorageUnits.size(BTreeNodePage.KEY_SIZE + BTreeNodePage.VALUE_SIZE);
        return StorageUnits.offset(pageSize - ((index + 1) * keyValuePairSize));
    }

    static @ByteOffset long getLogValueIndexOffset(final @ByteSize long pageSize, final int index)
    {
        return StorageUnits.offset(getLogKeyIndexOffset(pageSize, index) + BTreeNodePage.KEY_SIZE);
    }
}
