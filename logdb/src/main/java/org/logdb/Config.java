package org.logdb;

import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

public final class Config
{
    public static final @Version int LOG_DB_VERSION = StorageUnits.version(0);

    public static final @Version long INITIAL_STORAGE_VERSION = StorageUnits.version(0);
}
