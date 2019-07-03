package org.logdb.benchmark;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

final class DefaultBenchmarkConfig
{
    static final @ByteSize int PAGE_SIZE_BYTES = StorageUnits.size(4096); // default 4 KiBs

    static final @Version long INITIAL_VERSION = StorageUnits.version(0);
}