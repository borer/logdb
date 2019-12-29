package org.logdb;

import org.logdb.bbtree.BTree;
import org.logdb.bit.BinaryHelper;
import org.logdb.logfile.LogFile;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.io.IOException;
import java.util.Arrays;

import static org.logdb.bbtree.InvalidBTreeValues.KEY_NOT_FOUND_VALUE;

public class LogDb implements AutoCloseable
{
    private final LogFile logFile;
    private final BTree index;

    private byte[] buffer = new byte[Long.BYTES];

    public LogDb(final LogFile logFile, final BTree index)
    {
        this.logFile = logFile;
        this.index = index;
    }

    public void put(final byte[] key, final byte[] value) throws IOException
    {
        final @ByteOffset long offset = logFile.put(key, value);
        BinaryHelper.longToBytes(offset, buffer);
        index.put(key, buffer);
    }

    /**
     * Tries to retrieve the value for a given key.
     * @param key the key
     * @return the value for the given key or null if not found.
     */
    public byte[] get(final byte[] key)
    {
        final @ByteOffset byte[] offset = StorageUnits.offset(index.get(key));
        if (Arrays.equals(offset, KEY_NOT_FOUND_VALUE))
        {
            return null;
        }

        return logFile.read(StorageUnits.offset(BinaryHelper.bytesToLong(offset)));
    }

    /**
     * Tries to retrieve the value for a given key at a specific version.
     * @param key the key
     * @param version the version to search in
     * @return the value for the given key or null if not found
     */
    public byte[] get(final byte[] key, final @Version long version)
    {
        final byte[] value = index.get(key, version);
        final @ByteOffset long offset = StorageUnits.offset(BinaryHelper.bytesToLong(value));
        return logFile.read(offset);
    }

    public void delete(final byte[] key) throws IOException
    {
        logFile.delete(key);
        index.remove(key);
    }

    public void commitIndex() throws IOException
    {
        index.commit();
    }

    @Override
    public void close() throws Exception
    {
        logFile.close();
        index.close();
    }
}
