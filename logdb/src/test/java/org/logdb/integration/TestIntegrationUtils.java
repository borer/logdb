package org.logdb.integration;

import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bbtree.NodesManager;
import org.logdb.logfile.LogFile;
import org.logdb.storage.FileStorage;
import org.logdb.storage.FileStorageFactory;
import org.logdb.storage.FileType;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;

import static org.logdb.support.TestUtils.INITIAL_VERSION;
import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

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

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeWithLog(nodesManage, new StubTimeSource(), INITIAL_VERSION);
    }

    static BTreeWithLog loadPersistedLogBtree(final Path path)
    {
        LOGGER.info("Reading temporal path " + path.getFileName());

        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX);

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeWithLog(nodesManager, new StubTimeSource(), INITIAL_VERSION);
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

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeImpl(nodesManage, new StubTimeSource(), INITIAL_VERSION);
    }

    static BTreeImpl loadPersistedBtree(final Path path)
    {
        final FileStorage storage = FileStorageFactory.openExisting(path, FileType.INDEX);

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeImpl(nodesManager, new StubTimeSource(), INITIAL_VERSION);
    }
}
