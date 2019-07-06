package org.logdb.benchmark;

import org.logdb.bbtree.NodesManager;
import org.logdb.bbtree.RootReference;
import org.logdb.storage.StorageUnits;
import org.logdb.time.TimeUnits;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class BenchmarkUtils
{
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
            if (Files.isDirectory(file))
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
