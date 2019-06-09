package org.logdb;

import org.logdb.bbtree.BTree;
import org.logdb.logfile.LogFile;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LogDB
{
    private final LogFile logFile;
    private final BTree index;

    private ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);

    public LogDB(final LogFile logFile, final BTree index)
    {
        this.logFile = logFile;
        this.index = index;
    }

    public void put(final long key, final byte[] value)
    {
        final long offset = logFile.put(longToBytes(key), value);
        index.put(key, offset);
    }

    public byte[] get(final long key)
    {
        final long offset = index.get(key);
        return logFile.read(offset);
    }

    public byte[] get(final long key, final long version)
    {
        final long offset = index.get(key, version);
        return logFile.read(offset);
    }

    public void commitIndex()
    {
        index.commit();
    }

    private byte[] longToBytes(final long value)
    {
        keyBuffer.rewind();
        keyBuffer.putLong(value);
        return keyBuffer.array();
    }

    public void close()
    {
        try
        {
            logFile.close();
            index.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
