package org.borer.logdb;

import java.nio.ByteOrder;

public final class Config
{
    public static final int PAGE_SIZE_BYTES = 4096; // default 4 KiBs
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
}
