package org.logdb.benchmark;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

final class DefaultBenchmarkConfig
{
    static final @ByteSize int PAGE_SIZE_BYTES = StorageUnits.size(4096); // default 4 KiBs
}