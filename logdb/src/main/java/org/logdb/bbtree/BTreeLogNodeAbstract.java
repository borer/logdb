package org.logdb.bbtree;

import org.logdb.bit.BinaryHelper;
import org.logdb.bit.Memory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

import java.nio.charset.Charset;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public abstract class BTreeLogNodeAbstract extends BTreeNodeAbstract implements BTreeLogNode
{
    KeyValueLog keyValueLog;
    private @ByteSize int maxLogSize;

    BTreeLogNodeAbstract(
            final @PageNumber long pageNumber,
            final Memory memory,
            final @ByteSize int maxLogSize,
            final int numberOfPairs)
    {
        super(pageNumber, memory, numberOfPairs, StorageUnits.offset((short)(memory.getCapacity() - maxLogSize)));
        this.maxLogSize = maxLogSize;

        if (maxLogSize > 0)
        {
            final @ByteOffset long logOffset = getLogStartOffset(memory, maxLogSize);
            final Memory logMemory = memory.slice((int) logOffset);

            this.keyValueLog = KeyValueLogImpl.create(logMemory);
        }
        else
        {
            this.keyValueLog = KeyValueLogEmpty.INSTANCE;
        }
    }

    @ByteOffset
    static long getLogStartOffset(Memory memory, @ByteSize int maxLogSize)
    {
        return StorageUnits.offset(memory.getCapacity() - maxLogSize);
    }

    void refreshKeyValueLog()
    {
        keyValueLog.cacheNumberOfLogPairs();
    }

    int getNumberOfLogPairs()
    {
       return keyValueLog.getNumberOfPairs();
    }

    @Override
    public void reset()
    {
        super.reset();
        topKeyValueHeapOffset = StorageUnits.offset((short) (buffer.getCapacity() - maxLogSize));
        keyValueLog.resetLog();
    }

    @Override
    public int getLogKeyValuesCount()
    {
        return keyValueLog.getNumberOfPairs();
    }

    @Override
    public boolean shouldSplit(final @ByteSize int requiredSpace)
    {
        return requiredSpace > freeSizeLeftBytes;
    }

    @Override
    @ByteSize long calculateFreeSpaceLeft(final long pageSize)
    {
        final @ByteSize long sizeWithoutLog = StorageUnits.size(pageSize - maxLogSize);
        final @ByteSize int sizeForKeyValuesCells = StorageUnits.size(numberOfPairs * BTreeNodePage.CELL_SIZE);
        final @ByteSize long usedHeapSize = StorageUnits.size(sizeWithoutLog - topKeyValueHeapOffset);
        final long usedBytes = BTreeNodePage.HEADER_SIZE_BYTES + sizeForKeyValuesCells + usedHeapSize;
        return StorageUnits.size(sizeWithoutLog - usedBytes);
    }

    void splitLog(final byte[] key, final BTreeLogNodeAbstract bNode)
    {
        keyValueLog.splitLog(key, bNode.keyValueLog);

        recalculateFreeSpaceLeft();
        bNode.recalculateFreeSpaceLeft();
    }

    boolean logHasFreeSpace(final @ByteSize int sizeToInsert)
    {
        final @ByteSize long actualLogSize = keyValueLog.getUsedSize();
        return actualLogSize < maxLogSize && (maxLogSize - actualLogSize) > (sizeToInsert + KeyValueLogImpl.CELL_SIZE);
    }

    KeyValueLogImpl spillLog()
    {
        final KeyValueLogImpl spilledKeyValueLog = keyValueLog.spillLog();
        recalculateFreeSpaceLeft();
        return spilledKeyValueLog;
    }

    @Override
    public boolean hasKeyLog(final long key)
    {
        final int logIndex = keyValueLog.binarySearchInLog(BinaryHelper.longToBytes(key));
        return logIndex >= 0;
    }

    @Override
    public long getLogValue(final long key)
    {
        return getLogValue(BinaryHelper.longToBytes(key));
    }

    public long getLogValue(final byte[] key)
    {
        final int logIndex = keyValueLog.binarySearchInLog(key);
        if (logIndex >= 0)
        {
            return getLogValueAtIndex(logIndex);
        }
        else
        {
            return KEY_NOT_FOUND_VALUE;
        }
    }

    @Override
    public long getLogValueAtIndex(final int index)
    {
        return BinaryHelper.bytesToLong(getLogValueAtIndexBytes(index));
    }

    public byte[] getLogValueAtIndexBytes(final int index)
    {
        return keyValueLog.getValueBytesAtIndex(index);
    }

    long getLogKey(final int index)
    {
        return BinaryHelper.bytesToLong(getLogKeyBytes(index));
    }

    byte[] getLogKeyBytes(final int index)
    {
        return keyValueLog.getKeyBytesAtIndex(index);
    }

    int binarySearchInLog(final long key)
    {
        return binarySearchInLog(BinaryHelper.longToBytes(key));
    }

    int binarySearchInLog(final byte[] key)
    {
        return keyValueLog.binarySearchInLog(key);
    }

    void insertLog(final long key, final long value)
    {
        insertLog(BinaryHelper.longToBytes(key), BinaryHelper.longToBytes(value));
    }

    void insertLog(final byte[] key, final byte[] value)
    {
        keyValueLog.insertLog(key, value);
        recalculateFreeSpaceLeft();

        setDirty();
    }

    @Override
    public void removeLog(final long key)
    {
        removeLogBytes(BinaryHelper.longToBytes(key));
    }

    public void removeLogBytes(final byte[] key)
    {
        final boolean hasRemoved = keyValueLog.removeLogBytes(key);
        if (hasRemoved)
        {
            recalculateFreeSpaceLeft();
            setDirty();
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder(super.toString());

        final int numberOfLogPairs = keyValueLog.getNumberOfPairs();
        if (numberOfLogPairs > 0)
        {
            Charset utf8 = Charset.forName("UTF-8");
            contentBuilder.append(" log KV : ");
            for (int i = 0; i < numberOfLogPairs; i++)
            {
                contentBuilder.append(new String(getLogKeyBytes(i), utf8));
                contentBuilder.append("-");
                contentBuilder.append(new String(getLogValueAtIndexBytes(i), utf8));
                if (i + 1 != numberOfLogPairs)
                {
                    contentBuilder.append(",");
                }
            }
        }

        return contentBuilder.toString();
    }

    @Override
    String printDebug()
    {
        final StringBuilder contentBuilder = new StringBuilder(super.printDebug());
        if (keyValueLog instanceof KeyValueLogImpl)
        {
            final String logDebug = ((KeyValueLogImpl) keyValueLog).printDebug();
            return contentBuilder
                    .append(System.lineSeparator())
                    .append(logDebug)
                    .toString();
        }
        else
        {
            return contentBuilder.append("Empty Log").toString();
        }
    }
}
