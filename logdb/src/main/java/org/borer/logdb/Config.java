package org.borer.logdb;

import java.nio.ByteOrder;

public final class Config
{
    public static final int PAGE_SIZE_BYTES = 4096; // default 4 KiBs
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static final long MAPPED_CHUNK_SIZE = PAGE_SIZE_BYTES * 200;

    public static final long NUMBER_NODES_BEFORE_FLUSH = 10;
}
