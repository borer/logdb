package org.logdb.builder;

import org.logdb.LogDb;
import org.logdb.async.AsyncWriteDelegatingBTree;
import org.logdb.async.NonDaemonThreadFactory;
import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bbtree.NodesManager;
import org.logdb.bbtree.RootReference;
import org.logdb.bit.DirectMemory;
import org.logdb.checksum.Checksum;
import org.logdb.checksum.ChecksumFactory;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.logfile.LogFile;
import org.logdb.root.index.RootIndex;
import org.logdb.root.index.RootIndexRecord;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.file.FileStorage;
import org.logdb.storage.file.FileStorageFactory;
import org.logdb.storage.file.FileType;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeSource;
import org.logdb.time.TimeUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.logdb.bbtree.BTreeValidation.isNewTree;
import static org.logdb.storage.StorageUnits.INITIAL_VERSION;

public class LogDbBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LogDbBuilder.class);

    private Path rootDirectory;
    private @ByteSize long segmentFileSize;
    private ByteOrder byteOrder;
    private @ByteSize int pageSizeBytes;
    private TimeSource timeSource;
    private boolean useIndexWithLog;
    private boolean asyncIndexWrite;
    private int asyncQueueCapacity = 8192;
    private boolean shouldSyncWrite = false;
    private ChecksumType checksumType = ChecksumType.CRC32;
    private int nodeLogPercentage = 30;

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

    public LogDbBuilder useIndexWithLog(final boolean useIndexWithLog)
    {
        this.useIndexWithLog = useIndexWithLog;
        return this;
    }

    public LogDbBuilder asyncIndexWrite(final boolean asyncIndexWrite)
    {
        this.asyncIndexWrite = asyncIndexWrite;
        return this;
    }

    public LogDbBuilder asyncQueueCapacity(final int asyncQueueCapacity)
    {
        this.asyncQueueCapacity = asyncQueueCapacity;
        return this;
    }

    public LogDbBuilder shouldSyncWrite(final boolean shouldSyncWrite)
    {
        this.shouldSyncWrite = shouldSyncWrite;
        return this;
    }

    public LogDbBuilder checksum(final ChecksumType checksumType)
    {
        this.checksumType = checksumType;
        return this;
    }

    public LogDbBuilder nodeLogPercentage(final int nodeLogPercentage)
    {
        this.nodeLogPercentage = nodeLogPercentage;
        return this;
    }

    public LogDb build() throws IOException
    {
        validateConfig();

        LOGGER.info("Constructing LogDB");

        LOGGER.info("Starting constructing LogDB heap file");
        final LogFile logFile = buildLogFile(timeSource);
        LOGGER.info("Finnish constructing LogDB heap file");

        LOGGER.info("Starting constructing LogDB root index file");
        final RootIndex rootIndex = buildRootIndex();
        LOGGER.info("Finnish constructing LogDB root index file");

        LOGGER.info("Starting constructing LogDB index file");
        final BTree index = buildIndex(timeSource, rootIndex);
        LOGGER.info("Finnish constructing LogDB index file");

        final BTree indexToUse;
        if (asyncIndexWrite)
        {
            final AsyncWriteDelegatingBTree asyncWriteDelegatingBTree = new AsyncWriteDelegatingBTree(
                    new NonDaemonThreadFactory(),
                    index,
                    asyncQueueCapacity);
            asyncWriteDelegatingBTree.start();

            indexToUse = asyncWriteDelegatingBTree;
        }
        else
        {
            indexToUse = index;
        }

        return new LogDb(logFile, indexToUse);
    }

    private void validateConfig()
    {
        Objects.requireNonNull(rootDirectory);

        if (segmentFileSize < 0 || segmentFileSize % pageSizeBytes != 0)
        {
            throw new RuntimeException("invalid segment size, provided " + segmentFileSize);
        }
    }

    private RootIndex buildRootIndex() throws IOException
    {
        final FileStorage logDbRootIndexFileStorage = buildFileStorage(FileType.ROOT_INDEX);

        final @Version long version;
        final @Milliseconds long timestamp;
        final @ByteOffset long offset;
        final @PageNumber long lastRootPageNumber = logDbRootIndexFileStorage.getLastPersistedPageNumber();
        if (isNewTree(lastRootPageNumber))
        {
            version = INITIAL_VERSION;
            timestamp = TimeUnits.millis(0L);
            offset = StorageUnits.ZERO_OFFSET;
        }
        else
        {
            final @ByteOffset long lastPersistedOffset = logDbRootIndexFileStorage.getLastPersistedOffset();

            final @ByteOffset long offsetInsidePage = lastPersistedOffset - logDbRootIndexFileStorage.getOffset(lastRootPageNumber);
            final DirectMemory directMemory = logDbRootIndexFileStorage.getUninitiatedDirectMemoryPage();
            logDbRootIndexFileStorage.mapPage(lastRootPageNumber, directMemory);

            final RootIndexRecord lastRootIndexRecord = RootIndexRecord.read(directMemory, offsetInsidePage);
            version = lastRootIndexRecord.getVersion();
            timestamp = lastRootIndexRecord.getTimestamp();
            offset = lastRootIndexRecord.getOffset();
        }

        return new RootIndex(logDbRootIndexFileStorage, version, timestamp, offset);
    }

    private BTree buildIndex(final TimeSource timeSource, final RootIndex rootIndex) throws IOException
    {
        final FileStorage logDbIndexFileStorage = buildFileStorage(FileType.INDEX);
        final @Version long nextWriteVersion = getNextWriteVersion(logDbIndexFileStorage.getAppendVersion());
        final NodesManager nodesManager = new NodesManager(logDbIndexFileStorage, rootIndex, shouldSyncWrite);

        final @PageNumber long lastRootPageNumber = nodesManager.loadLastRootPageNumber();
        final RootReference rootReference;
        if (isNewTree(lastRootPageNumber))
        {
            rootReference = new RootReference(
                    nodesManager.createEmptyLeafNode(),
                    timeSource.getCurrentMillis(),
                    StorageUnits.version(INITIAL_VERSION - 1),
                    null);
        }
        else
        {
            rootReference = null;
        }

        final BTree index;
        if (useIndexWithLog)
        {
            index = new BTreeWithLog(nodesManager, timeSource, nextWriteVersion, lastRootPageNumber, rootReference, nodeLogPercentage);
        }
        else
        {
            index = new BTreeImpl(nodesManager, timeSource, nextWriteVersion, lastRootPageNumber, rootReference);
        }

        return index;
    }

    private LogFile buildLogFile(final TimeSource timeSource) throws IOException
    {
        final FileStorage logDbFileStorage = buildFileStorage(FileType.HEAP);
        final @Version long nextWriteVersion = getNextWriteVersion(logDbFileStorage.getAppendVersion());

        final Checksum checksum = ChecksumFactory.checksumFromType(checksumType);
        final ChecksumHelper checksumHelper = new ChecksumHelper(checksum, checksumType);

        return new LogFile(logDbFileStorage, timeSource, nextWriteVersion, shouldSyncWrite, checksumHelper);
    }

    private @Version long getNextWriteVersion(final @Version long appendVersion)
    {
        return appendVersion == INITIAL_VERSION ? INITIAL_VERSION : StorageUnits.version(appendVersion + 1);
    }

    private FileStorage buildFileStorage(final FileType fileType) throws IOException
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
                    pageSizeBytes,
                    checksumType);
        }
        else
        {
            fileStorage = FileStorageFactory.openExisting(rootDirectory, fileType, checksumType);
        }
        return fileStorage;
    }
}
