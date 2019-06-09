package org.logdb.time;

public interface TimeSource
{
    @Milliseconds long getCurrentMillis();
}
