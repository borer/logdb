package org.logdb;

import org.logdb.storage.Version;
import org.logdb.storage.VersionUnit;

public final class Config
{
    public static final @Version long LOG_DB_VERSION = VersionUnit.version(1);
}
