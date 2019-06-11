package org.logdb;

import org.logdb.bbtree.BTree;
import org.logdb.logfile.LogFile;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.NodesManager;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LogDB
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodesManager.class);

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
        final @ByteOffset long offset = logFile.put(longToBytes(key), value);
        index.put(key, offset);
    }

    public byte[] get(final long key)
    {
        final @ByteOffset long offset = StorageUnits.offset(index.get(key));
        return logFile.read(offset);
    }

    public byte[] get(final long key, final @Version long version)
    {
        final @ByteOffset long offset = StorageUnits.offset(index.get(key, version));
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
        catch (final IOException e)
        {
            LOGGER.error("Unable to close DB", e);
        }
    }
}
