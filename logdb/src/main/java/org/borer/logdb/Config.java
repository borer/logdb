package org.borer.logdb;

public final class Config
{
    public static final int PAGE_SIZE_BYTES = 4096; // default 4 KiBs

    public static final int LOG_DB_VERSION = 1;

    //To ELiminate
    public static final long NUMBER_NODES_BEFORE_FLUSH = 10;
}
