package org.logdb;

import org.logdb.bbtree.BTreeWithLog;
import org.logdb.logfile.LogFile;
import org.logdb.storage.ByteSize;
import org.logdb.storage.FileStorage;
import org.logdb.storage.NodesManager;
import org.logdb.time.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

public class LogDbBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogDbBuilder.class);

    private String rootDirectory;
    private String dbName;
    private @ByteSize long memoryMappedChunkSizeBytes;
    private ByteOrder byteOrder;
    private @ByteSize int pageSizeBytes;
    private TimeSource timeSource;

    public LogDbBuilder setRootDirectory(String rootDirectory)
    {
        this.rootDirectory = rootDirectory;
        return this;
    }

    public LogDbBuilder setDbName(String dbName)
    {
        this.dbName = dbName;
        return this;
    }

    public LogDbBuilder setMemoryMappedChunkSizeBytes(final @ByteSize long memoryMappedChunkSizeBytes)
    {
        this.memoryMappedChunkSizeBytes = memoryMappedChunkSizeBytes;
        return this;
    }

    public LogDbBuilder setByteOrder(final ByteOrder byteOrder)
    {
        this.byteOrder = byteOrder;
        return this;
    }

    public LogDbBuilder setPageSizeBytes(final @ByteSize int pageSizeBytes)
    {
        this.pageSizeBytes = pageSizeBytes;
        return this;
    }

    public LogDbBuilder setTimeSource(final TimeSource timeSource)
    {
        this.timeSource = timeSource;
        return this;
    }

    public LogDb build() throws IOException
    {
        LOGGER.info("Constructing LogDB");

        LOGGER.info("Starting constructing LogDB heap file");
        final LogFile logFile = buildLogFile(timeSource);
        LOGGER.info("Finnish constructing LogDB heap file");

        LOGGER.info("Starting constructing LogDB index file");
        final BTreeWithLog bTreeWithLog = buildIndex(timeSource);
        LOGGER.info("Finnish constructing LogDB index file");

        return new LogDb(logFile, bTreeWithLog);
    }

    private BTreeWithLog buildIndex(TimeSource timeSource) throws IOException
    {
        final FileStorage logDbIndexFileStorage;
        logDbIndexFileStorage = buildFileStorage(".logIndex", "Unable to create log index file ");

        final NodesManager nodesManager = new NodesManager(logDbIndexFileStorage);
        return new BTreeWithLog(nodesManager, timeSource);
    }

    private LogFile buildLogFile(TimeSource timeSource) throws IOException
    {
        final FileStorage logDbFileStorage = buildFileStorage(".logdb", "Unable to create logdb file ");
        return new LogFile(logDbFileStorage, timeSource);
    }

    private FileStorage buildFileStorage(final String fileExtension, final String errorMessage) throws IOException
    {
        final FileStorage logDbIndexFileStorage;
        final File logDbIndexFile = new File(rootDirectory, dbName + fileExtension);
        if (!logDbIndexFile.exists())
        {
            logDbIndexFileStorage = FileStorage.createNewFileDb(
                    logDbIndexFile,
                    memoryMappedChunkSizeBytes,
                    byteOrder,
                    pageSizeBytes);
        }
        else
        {
            logDbIndexFileStorage = FileStorage.openDbFile(logDbIndexFile);
        }
        return logDbIndexFileStorage;
    }
}
