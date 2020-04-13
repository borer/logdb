package org.logdb;

import org.logdb.storage.Version;

import java.io.IOException;

public interface Index extends AutoCloseable
{
    void put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    byte[] get(byte[] key, @Version long version);

    void remove(byte[] key);

    void commit() throws IOException;
}
