package org.logdb.integration;

import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bbtree.NodesManager;
import org.logdb.bit.DirectMemory;
import org.logdb.logfile.LogFile;
import org.logdb.root.index.RootIndex;
import org.logdb.root.index.RootIndexRecord;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.file.FileStorage;
import org.logdb.storage.file.FileStorageFactory;
import org.logdb.storage.file.FileType;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.logdb.time.Milliseconds;
import org.logdb.time.TimeUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;
import static org.logdb.support.TestUtils.createInitialRootReference;

class TestIntegrationUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestIntegrationUtils.class);

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
                PAGE_SIZE_BYTES);

        return new LogFile(storageLogFile, new StubTimeSource(), storageLogFile.getAppendVersion());
    }

    static LogFile readLogFile(final Path rootDirectory)
    {
        FileStorage storageLogFile = FileStorageFactory.openExisting(rootDirectory, FileType.HEAP);
        return new LogFile(storageLogFile, new StubTimeSource(), storageLogFile.getAppendVersion());
    }

    static BTreeWithLog createNewPersistedLogBtree(final Path path) throws IOException
    {
        return createNewPersistedLogBtree(path, TestUtils.BYTE_ORDER);
    }

    static BTreeWithLog createNewPersistedLogBtree(final Path path, final ByteOrder byteOrder) throws IOException
    {
        LOGGER.info("Creating temporal path " + path.getFileName());

        final FileStorage storage = FileStorageFactory.createNew(
                path,
                FileType.INDEX,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES);

        final RootIndex rootIndex = createRootIndex(path, byteOrder);
        final NodesManager nodesManage = new NodesManager(storage, rootIndex);

        return new BTreeWithLog(
                nodesManage,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManage));
    }

    static BTreeWithLog loadPersistedLogBtree(final Path path)
    {
        LOGGER.info("Reading temporal path " + path.getFileName());

        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX);

        final RootIndex rootIndex = openRootIndex(path);
        final NodesManager nodesManager = new NodesManager(storage, rootIndex);

        return new BTreeWithLog(
                nodesManager,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                nodesManager.loadLastRootPageNumber(),
                null);
    }

    static BTreeImpl createNewPersistedBtree(final Path path) throws IOException
    {
        return createNewPersistedBtree(path, TestUtils.BYTE_ORDER);
    }

    static BTreeImpl createNewPersistedBtree(final Path path, final ByteOrder byteOrder) throws IOException
    {
        final FileStorage storage = FileStorageFactory.createNew(
                path,
                FileType.INDEX,
                TestUtils.SEGMENT_FILE_SIZE,
                byteOrder,
                PAGE_SIZE_BYTES);

        final RootIndex rootIndex = createRootIndex(path, byteOrder);
        final NodesManager nodesManage = new NodesManager(storage, rootIndex);

        return new BTreeImpl(
                nodesManage,
                rootIndex,
                new StubTimeSource(),
                INITIAL_VERSION,
                StorageUnits.INVALID_PAGE_NUMBER,
                createInitialRootReference(nodesManage));
    }

    static BTreeImpl loadPersistedBtree(final Path path)
    {
        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX);

        final RootIndex rootIndex = openRootIndex(path);
        final NodesManager nodesManager = new NodesManager(storage, rootIndex);

        return new BTreeImpl(
                nodesManager,
                rootIndex,
                new StubTimeSource(),
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
                PAGE_SIZE_BYTES);

        return new RootIndex(
                rootIndexStorage,
                StorageUnits.INITIAL_VERSION,
                TimeUnits.millis(0L),
                StorageUnits.ZERO_OFFSET);
    }

    private static RootIndex openRootIndex(final Path path)
    {
        final FileStorage rootIndexStorage = FileStorageFactory.openExisting(path, FileType.ROOT_INDEX);

        final @ByteOffset long lastPersistedOffset = rootIndexStorage.getLastPersistedOffset();
        final @PageNumber long lastRootPageNumber = rootIndexStorage.getPageNumber(lastPersistedOffset);

        @ByteOffset long offsetInsidePage = lastPersistedOffset - rootIndexStorage.getOffset(lastRootPageNumber);
        DirectMemory directMemory = rootIndexStorage.getUninitiatedDirectMemoryPage();
        rootIndexStorage.mapPage(lastRootPageNumber, directMemory);

        final @ByteOffset long versionOffset = RootIndexRecord.versionOffset(offsetInsidePage);
        final @Version long version = StorageUnits.version(directMemory.getLong(versionOffset));

        final @ByteOffset long timestampOffset = RootIndexRecord.timestampOffset(offsetInsidePage);
        final @Milliseconds long timestamp = TimeUnits.millis(directMemory.getLong(timestampOffset));

        final @ByteOffset long offsetOffset = RootIndexRecord.offsetValueOffset(offsetInsidePage);
        final @ByteOffset long offset = StorageUnits.offset(directMemory.getLong(offsetOffset));

        return new RootIndex(rootIndexStorage, version, timestamp, offset);
    }
}
