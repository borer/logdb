package org.borer.logdb;

public final class Config
{
    public static final int PAGE_SIZE_BYTES = 4096; // default 4 KiBs

    //TODO: this should be worked out from the page header size and the page header
    @Deprecated
    public static final int MAX_CHILDREN_PER_NODE = 10;

    public static final int LOG_DB_VERSION = 1;
}
