package org.logdb.integration;

import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bbtree.NodesManager;
import org.logdb.bit.DirectMemory;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;
import org.logdb.logfile.LogFile;
import org.logdb.root.index.RootIndex;
import org.logdb.root.index.RootIndexRecord;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorage;
import org.logdb.storage.file.FileStorageFactory;
import org.logdb.storage.file.FileType;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.logdb.time.TimeSource;
import org.logdb.time.TimeUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.NODE_LOG_PERCENTAGE;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;
import static org.logdb.support.TestUtils.createInitialRootReference;

class TestIntegrationUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestIntegrationUtils.class);
    private static final Crc32 CHECKSUM = new Crc32();
    private static final ChecksumType CHECKSUM_TYPE = ChecksumType.CRC32;
    private static final ChecksumHelper CHECKSUM_HELPER = new ChecksumHelper(CHECKSUM, CHECKSUM_TYPE);

    static LogFile createNewLogFile(final Path file) throws IOException
    {
        return createNewLogFile(file, TestUtils.BYTE_ORDER);
    }

    static LogFile createNewLogFile(final Path rootDirectory, final ByteOrder byteOrder) throws IOException
    {
        FileStorage storageLogFile = FileStorageFactory.createNew(
                rootDirectory,
                FileType.HEAP,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES,
                CHECKSUM_TYPE);

        return new LogFile(storageLogFile, new StubTimeSource(), storageLogFile.getAppendVersion(), true, CHECKSUM_HELPER);
    }

    static LogFile readLogFile(final Path rootDirectory)
    {
        FileStorage storageLogFile = FileStorageFactory.openExisting(rootDirectory, FileType.HEAP, CHECKSUM_TYPE);
        return new LogFile(storageLogFile, new StubTimeSource(), storageLogFile.getAppendVersion(), true, CHECKSUM_HELPER);
    }

    static BTreeWithLog createNewPersistedLogBtree(final Path path) throws IOException
    {
        return createNewPersistedLogBtree(path, TestUtils.BYTE_ORDER, new StubTimeSource());
    }

    static BTreeWithLog createNewPersistedLogBtree(final Path path, final ByteOrder byteOrder) throws IOException
    {
        return createNewPersistedLogBtree(path, byteOrder, new StubTimeSource());
    }

    static BTreeWithLog createNewPersistedLogBtree(
            final Path path,
            final ByteOrder byteOrder,
            final TimeSource timeSource) throws IOException
    {
        LOGGER.info("Creating temporal path " + path.getFileName());

        final FileStorage storage = FileStorageFactory.createNew(
                path,
                FileType.INDEX,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES,
                CHECKSUM_TYPE);

        final RootIndex rootIndex = createRootIndex(path, byteOrder);
        final NodesManager nodesManage = new NodesManager(storage, rootIndex, true);

        return new BTreeWithLog(
                nodesManage,
                timeSource,
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManage),
                NODE_LOG_PERCENTAGE);
    }

    static BTreeWithLog loadPersistedLogBtree(final Path path)
    {
        LOGGER.info("Reading temporal path " + path.getFileName());

        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX, CHECKSUM_TYPE);

        final RootIndex rootIndex = openRootIndex(path);
        final NodesManager nodesManager = new NodesManager(storage, rootIndex, true);

        return new BTreeWithLog(
                nodesManager,
                new StubTimeSource(),
                INITIAL_VERSION,
                nodesManager.loadLastRootPageNumber(),
                null,
                NODE_LOG_PERCENTAGE);
    }

    static BTreeImpl createNewPersistedBtree(final Path path, final TimeSource timeSource) throws IOException
    {
        return createNewPersistedBtree(path, TestUtils.BYTE_ORDER, timeSource);
    }

    static BTreeImpl createNewPersistedBtree(final Path path) throws IOException
    {
        return createNewPersistedBtree(path, TestUtils.BYTE_ORDER, new StubTimeSource());
    }

    static BTreeImpl createNewPersistedBtree(final Path path, final ByteOrder byteOrder) throws IOException
    {
        return createNewPersistedBtree(path, byteOrder, new StubTimeSource());
    }

    static BTreeImpl createNewPersistedBtree(
            final Path path,
            final ByteOrder byteOrder,
            final TimeSource timeSource) throws IOException
    {
        final FileStorage storage = FileStorageFactory.createNew(
                path,
                FileType.INDEX,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES,
                CHECKSUM_TYPE);

        final RootIndex rootIndex = createRootIndex(path, byteOrder);
        final NodesManager nodesManage = new NodesManager(storage, rootIndex, true);

        return new BTreeImpl(
                nodesManage,
                timeSource,
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManage));
    }

    static BTreeImpl loadPersistedBtree(final Path path)
    {
        return loadPersistedBtree(path, new StubTimeSource());
    }

    static BTreeImpl loadPersistedBtree(final Path path, final TimeSource timeSource)
    {
        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX, CHECKSUM_TYPE);

        final RootIndex rootIndex = openRootIndex(path);
        final NodesManager nodesManager = new NodesManager(storage, rootIndex, true);

        return new BTreeImpl(
                nodesManager,
                timeSource,
                INITIAL_VERSION,
                nodesManager.loadLastRootPageNumber(),
                null);
    }

    private static RootIndex createRootIndex(final Path path, final ByteOrder byteOrder) throws IOException
    {
        final FileStorage rootIndexStorage = FileStorageFactory.createNew(
                path,
                FileType.ROOT_INDEX,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES,
                CHECKSUM_TYPE);

        return new RootIndex(
                rootIndexStorage,
                StorageUnits.INITIAL_VERSION,
                TimeUnits.millis(0L),
                StorageUnits.ZERO_OFFSET);
    }

    private static RootIndex openRootIndex(final Path path)
    {
        final FileStorage rootIndexStorage = FileStorageFactory.openExisting(path, FileType.ROOT_INDEX, CHECKSUM_TYPE);

        final @ByteOffset long lastPersistedOffset = rootIndexStorage.getLastPersistedOffset();
        final @PageNumber long lastRootPageNumber = rootIndexStorage.getPageNumber(lastPersistedOffset);

        @ByteOffset long offsetInsidePage = lastPersistedOffset - rootIndexStorage.getOffset(lastRootPageNumber);
        DirectMemory directMemory = rootIndexStorage.getUninitiatedDirectMemoryPage();
        rootIndexStorage.mapPage(lastRootPageNumber, directMemory);

        final RootIndexRecord rootIndexRecord = RootIndexRecord.read(directMemory, offsetInsidePage);

        return new RootIndex(
                rootIndexStorage,
                rootIndexRecord.getVersion(),
                rootIndexRecord.getTimestamp(),
                rootIndexRecord.getOffset());
    }
}
