package org.logdb.benchmark;

import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;

import java.nio.ByteOrder;

final class DefaultBenchmarkConfig
{
    static final @ByteSize int PAGE_SIZE_BYTES = StorageUnits.size(4096); // default 4 KiBs
    static final @ByteSize int PAGE_LOG_SIZE_BYTES = StorageUnits.size(2048); // default 2 KiBs
    static final @ByteSize int NODE_LOG_SIZE = StorageUnits.size(1228); // default 30% of page size

    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    static final @Version long INITIAL_VERSION = StorageUnits.version(0);

    static final @ByteSize long SEGMENT_FILE_SIZE = StorageUnits.size(Integer.MAX_VALUE - 4095); //max file segment

}