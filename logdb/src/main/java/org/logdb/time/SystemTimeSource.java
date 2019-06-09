package org.logdb.time;

public class SystemTimeSource implements TimeSource
{
    @Override
    public @Milliseconds long getCurrentMillis()
    {
        return TimeUnits.millis(System.currentTimeMillis());
    }
}
