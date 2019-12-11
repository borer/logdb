package org.logdb.async;

import org.logdb.bbtree.BTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class CommandProcessingThread implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandProcessingThread.class);

    private final ManyToOneConcurrentArrayQueue<Command> queue;
    private final BTree delegate;
    private int numberOfModification;
    private boolean isRunning;

    CommandProcessingThread(final ManyToOneConcurrentArrayQueue<Command> queue, final BTree delegate)
    {
        this.queue = queue;
        this.delegate = delegate;
        this.numberOfModification = 0;
        this.isRunning = false;
    }
    @Override
    public void run()
    {
        while (isRunning)
        {
            queue.drain(command ->
            {
                ++numberOfModification;
                switch (command.commandType)
                {
                    case ADD:
                        try
                        {
                            delegate.put(command.key, command.value);
                        }
                        catch (ArrayIndexOutOfBoundsException e)
                        {
                            LOGGER.error("index out of bounds for " + command.toString(), e);
                        }
                        break;
                    case DELETE:
                        delegate.remove(command.key);
                        break;
                    default:
                        throw new RuntimeException("Unrecognized command " + command.toString());
                }
            });

            if (numberOfModification > 0)
            {
                numberOfModification = 0;
                try
                {
                    delegate.commit();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Unable to commit delegate ", e);
                }
            }
        }
    }

    void stop()
    {
        isRunning = false;
    }

    boolean isRunning()
    {
        return isRunning;
    }
}
