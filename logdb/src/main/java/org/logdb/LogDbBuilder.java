package org.logdb;

import org.logdb.bbtree.BTreeWithLog;
import org.logdb.logfile.LogFile;
import org.logdb.storage.ByteSize;
import org.logdb.storage.FileStorage;
import org.logdb.storage.FileStorageFactory;
import org.logdb.storage.FileType;
import org.logdb.storage.NodesManager;
import org.logdb.time.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

public class LogDbBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogDbBuilder.class);

    private Path rootDirectory;
    private @ByteSize long segmentFileSize;
    private ByteOrder byteOrder;
    private @ByteSize int pageSizeBytes;
    private TimeSource timeSource;

    public LogDbBuilder setRootDirectory(final Path rootDirectory)
    {
        this.rootDirectory = rootDirectory;
        return this;
    }

    public LogDbBuilder setSegmentFileSize(final @ByteSize long segmentFileSize)
    {
        this.segmentFileSize = segmentFileSize;
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
        final FileStorage logDbIndexFileStorage = buildFileStorage(FileType.INDEX);

        final NodesManager nodesManager = new NodesManager(logDbIndexFileStorage);
        return new BTreeWithLog(nodesManager, timeSource);
    }

    private LogFile buildLogFile(TimeSource timeSource) throws IOException
    {
        final FileStorage logDbFileStorage = buildFileStorage(FileType.HEAP);
        return new LogFile(logDbFileStorage, timeSource);
    }

    private FileStorage buildFileStorage(FileType fileType) throws IOException
    {
        final FileStorage fileStorage;
        if (!Files.exists(rootDirectory) || Files.list(rootDirectory).noneMatch(fileType))
        {
            Files.createDirectories(rootDirectory);

            fileStorage = FileStorageFactory.createNew(
                    rootDirectory,
                    fileType,
                    segmentFileSize,
                    byteOrder,
                    pageSizeBytes);
        }
        else
        {
            fileStorage = FileStorageFactory.openExisting(rootDirectory, fileType);
        }
        return fileStorage;
    }
}
