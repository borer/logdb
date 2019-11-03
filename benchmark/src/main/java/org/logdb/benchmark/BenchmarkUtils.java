package org.logdb.benchmark;

import org.logdb.bbtree.NodesManager;
import org.logdb.bbtree.RootReference;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorage;
import org.logdb.storage.file.FileStorageFactory;
import org.logdb.storage.file.FileType;
import org.logdb.time.TimeUnits;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

class BenchmarkUtils
{
    static RootIndex createRootIndex(
            final Path path,
            final @ByteSize long segmentFileSize,
            final @ByteSize int pageSize,
            final ByteOrder byteOrder) throws IOException
    {
        final FileStorage rootIndexStorage = FileStorageFactory.createNew(
                path,
                FileType.ROOT_INDEX,
                segmentFileSize,
                byteOrder,
                pageSize);

        return new RootIndex(
                rootIndexStorage,
                StorageUnits.INITIAL_VERSION,
                TimeUnits.millis(0L),
                StorageUnits.ZERO_OFFSET);
    }

    static RootReference createInitialRootReference(final NodesManager nodesManager)
    {
        return new RootReference(
                nodesManager.createEmptyLeafNode(),
                TimeUnits.millis(0),
                StorageUnits.INITIAL_VERSION,
                null);
    }

    static void removeAllFilesFromDirectory(Path rootDirectory) throws IOException
    {
        Files.list(rootDirectory).forEach(file ->
        {
            if (Files.isRegularFile(file))
            {
                try
                {
                    Files.deleteIfExists(file);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        });
    }
}
