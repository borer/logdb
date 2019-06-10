package org.logdb;

import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

public final class Config
{
    public static final @Version long LOG_DB_VERSION = StorageUnits.version(1);
}
