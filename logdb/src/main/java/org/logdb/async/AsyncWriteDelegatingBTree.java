package org.logdb.async;

import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeNode;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Version;
import org.logdb.time.Milliseconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

public class AsyncWriteDelegatingBTree implements BTree
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWriteDelegatingBTree.class);

    private static final byte[] INVALID_VALUE = new byte[0];
    private final ManyToOneConcurrentArrayQueue<Command> queue;
    private final ThreadFactory threadFactory;
    private final BTree delegate;
    private Thread thread;
    private final CommandProcessingThread commandProcessingThread;

    public AsyncWriteDelegatingBTree(
            final ThreadFactory threadFactory,
            final BTree delegate,
            final int queueCapacity)
    {
        this.threadFactory = threadFactory;
        this.delegate = delegate;
        this.queue = new ManyToOneConcurrentArrayQueue<>(queueCapacity);
        this.commandProcessingThread = new CommandProcessingThread(queue, delegate);
    }

    public void start()
    {
        if (commandProcessingThread.isRunning())
        {
            return;
        }

        thread = threadFactory.newThread(commandProcessingThread);
        thread.start();
    }

    private void stop()
    {
        try
        {
            commandProcessingThread.stop();
            thread.join();
        }
        catch (final InterruptedException e)
        {
            LOGGER.error("InterruptedException while trying to stop index writing thread", e);
        }
    }

    @Override
    public void remove(final byte[] key)
    {
        sendCommandToQueue(new Command(CommandType.DELETE, key, INVALID_VALUE));
    }

    @Override
    public void put(final byte[] key, final byte[] value)
    {
        sendCommandToQueue(new Command(CommandType.ADD, key, value));
    }

    private void sendCommandToQueue(final Command command)
    {
        boolean isAdded = false;
        while (!isAdded)
        {
            isAdded = queue.offer(command);
        }
    }

    @Override
    public byte[] get(final byte[] key, final @Version long version)
    {
        return delegate.get(key, version);
    }

    @Override
    public byte[] get(final byte[] key)
    {
        return delegate.get(key);
    }

    @Override
    public byte[] getByTimestamp(final byte[] key, @Milliseconds long timestamp)
    {
        return delegate.getByTimestamp(key, timestamp);
    }

    @Override
    public void commit() throws IOException
    {
        //NO-OP
    }

    @Override
    public String print()
    {
        return delegate.print();
    }

    @Override
    public long getNodesCount()
    {
        return delegate.getNodesCount();
    }

    @Override
    public @PageNumber long getCommittedRoot()
    {
        return delegate.getCommittedRoot();
    }

    @Override
    public BTreeNode getUncommittedRoot()
    {
        return delegate.getUncommittedRoot();
    }

    @Override
    public void close() throws Exception
    {
        stop();
        delegate.close();
    }
}
