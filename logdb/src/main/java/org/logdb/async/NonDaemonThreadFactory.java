package org.logdb.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

public class NonDaemonThreadFactory implements ThreadFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NonDaemonThreadFactory.class);

    @Override
    public Thread newThread(final Runnable runnable)
    {
        final Thread thread = new Thread(runnable);
        thread.setDaemon(false);
        thread.setUncaughtExceptionHandler(
                (t, e) -> LOGGER.error(
                        String.format("Thread %s throw an uncaught exception", t.getName()),
                        e)
        );

        return thread;
    }
}
