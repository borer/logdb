package org.logdb.support;

import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;

public class StubTimeSource implements TimeSource
{
    long currentTime = 0;

    @Override
    public @Milliseconds long getCurrentMillis()
    {
        currentTime++;

        return currentTime;
    }
}