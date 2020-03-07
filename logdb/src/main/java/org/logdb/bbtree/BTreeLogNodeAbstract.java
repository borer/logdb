package org.logdb.bbtree;

import org.logdb.bit.Memory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;

import java.nio.charset.StandardCharsets;

public abstract class BTreeLogNodeAbstract extends BTreeNodeAbstract implements BTreeLogNode
{
    KeyValueHeap logHeap;
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

            this.logHeap = KeyValueHeapImpl.create(logMemory);
        }
        else
        {
            this.logHeap = KeyValueHeapEmpty.INSTANCE;
        }
    }

    @ByteOffset
    static long getLogStartOffset(Memory memory, @ByteSize int maxLogSize)
    {
        return StorageUnits.offset(memory.getCapacity() - maxLogSize);
    }

    void refreshKeyValueLog()
    {
        logHeap.cacheNumberOfLogPairs();
    }

    int getNumberOfLogPairs()
    {
       return logHeap.getNumberOfPairs();
    }

    @Override
    public void reset()
    {
        super.reset();
        entries.reset((short)1);
        logHeap.reset((short)0);
    }

    @Override
    public int getLogKeyValuesCount()
    {
        return logHeap.getNumberOfPairs();
    }

    @Override
    @ByteSize long calculateFreeSpaceLeft(final @ByteSize long pageSize)
    {
        final @ByteSize long sizeWithoutLog = StorageUnits.size(pageSize - maxLogSize);
        final @ByteSize long usedHeapSize = entries.getUsedSize();
        final @ByteSize long usedBytes = StorageUnits.size(BTreeNodePage.PAGE_HEADER_SIZE + usedHeapSize);
        return StorageUnits.size(sizeWithoutLog - usedBytes);
    }

    void splitLog(final byte[] key, final BTreeLogNodeAbstract bNode)
    {
        logHeap.split(key, bNode.logHeap);
    }

    boolean logHasFreeSpace(final @ByteSize int sizeToInsert)
    {
        final @ByteSize long actualLogSize = logHeap.getUsedSize();
        return actualLogSize < maxLogSize && (maxLogSize - actualLogSize) > (sizeToInsert + KeyValueHeapImpl.CELL_SIZE);
    }

    KeyValueHeapImpl spillLog()
    {
        return logHeap.spill();
    }

    @Override
    public boolean hasKeyLog(final byte[] key)
    {
        final int logIndex = logHeap.binarySearch(key);
        return logIndex >= 0;
    }

    @Override
    public byte[] getLogValue(final byte[] key)
    {
        final int logIndex = logHeap.binarySearch(key);
        if (logIndex >= 0)
        {
            return getLogValueAtIndex(logIndex);
        }
        else
        {
            return null;
        }
    }

    @Override
    public byte[] getLogValueAtIndex(final int index)
    {
        return logHeap.getValueAtIndex(index);
    }

    byte[] getLogKey(final int index)
    {
        return logHeap.getKeyAtIndex(index);
    }

    int binarySearchInLog(final byte[] key)
    {
        return logHeap.binarySearch(key);
    }

    void insertLog(final byte[] key, final byte[] value)
    {
        logHeap.insert(key, value);

        setDirty();
    }

    @Override
    public void removeLog(final byte[] key)
    {
        final boolean hasRemoved = logHeap.removeKeyValue(key);
        if (hasRemoved)
        {
            setDirty();
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder contentBuilder = new StringBuilder(super.toString());

        final int numberOfLogPairs = logHeap.getNumberOfPairs();
        if (numberOfLogPairs > 0)
        {
            contentBuilder.append(" log KV : ");
            for (int i = 0; i < numberOfLogPairs; i++)
            {
                contentBuilder.append(new String(getLogKey(i), StandardCharsets.UTF_8));
                contentBuilder.append("-");
                contentBuilder.append(new String(getLogValueAtIndex(i), StandardCharsets.UTF_8));
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
        if (logHeap instanceof KeyValueHeapImpl)
        {
            return contentBuilder
                    .append(System.lineSeparator())
                    .append(logHeap.printDebug())
                    .toString();
        }
        else
        {
            return contentBuilder.append("Empty Log").toString();
        }
    }
}
