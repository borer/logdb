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

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

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

    /**
     * Tries to retrieve the value for a given key.
     * @param key the key
     * @return the value for the given key or null if not found.
     */
    public byte[] get(final long key)
    {
        final @ByteOffset long offset = StorageUnits.offset(index.get(key));
        if (offset == KEY_NOT_FOUND_VALUE)
        {
            return null;
        }

        return logFile.read(offset);
    }

    /**
     * Tries to retrieve the value for a given key at a specific version.
     * @param key the key
     * @param version the version to search in
     * @return the value for the given key or null if not found
     */
    public byte[] get(final long key, final @Version long version)
    {
        final @ByteOffset long offset = StorageUnits.offset(index.get(key, version));
        return logFile.read(offset);
    }

    public void delete(final long key)
    {
        logFile.remove(longToBytes(key));
        index.remove(key);
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
