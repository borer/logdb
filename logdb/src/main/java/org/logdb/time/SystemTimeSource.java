package org.logdb.time;

public class SystemTimeSource implements TimeSource
{
    @Override
    public long getCurrentMillis()
    {
        return System.currentTimeMillis();
    }
}
