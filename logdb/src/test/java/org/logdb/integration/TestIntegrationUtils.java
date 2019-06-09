package org.logdb.integration;

import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.BTreeWithLog;
import org.logdb.logfile.LogFile;
import org.logdb.storage.FileStorage;
import org.logdb.storage.NodesManager;
import org.logdb.support.StubTimeSource;
import org.logdb.support.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.logdb.support.TestUtils.PAGE_SIZE_BYTES;

class TestIntegrationUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestIntegrationUtils.class);

    static LogFile createNewLogFile(final File file)
    {
        FileStorage storageLogFile = FileStorage.createNewFileDb(
                file,
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        return new LogFile(storageLogFile, new StubTimeSource());
    }

    static BTreeWithLog createNewPersistedLogBtree(final File file)
    {
        LOGGER.info("Creating temporal path " + file.getAbsolutePath());

        final FileStorage storage = FileStorage.createNewFileDb(
                file,
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeWithLog(nodesManage, new StubTimeSource());
    }

    static BTreeWithLog loadPersistedLogBtree(final File file)
    {
        LOGGER.info("Reading temporal path " + file.getAbsolutePath());

        final FileStorage storage = FileStorage.openDbFile(file);

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeWithLog(nodesManager, new StubTimeSource());
    }

    static BTreeImpl createNewPersistedBtree(final File file)
    {
        final FileStorage storage = FileStorage.createNewFileDb(
                file,
                TestUtils.MAPPED_CHUNK_SIZE,
                TestUtils.BYTE_ORDER,
                PAGE_SIZE_BYTES);

        final NodesManager nodesManage = new NodesManager(storage);

        return new BTreeImpl(nodesManage, new StubTimeSource());
    }

    static BTreeImpl loadPersistedBtree(final File file)
    {
        final FileStorage storage = FileStorage.openDbFile(file);

        final NodesManager nodesManager = new NodesManager(storage);
        return new BTreeImpl(nodesManager, new StubTimeSource());
    }
}
