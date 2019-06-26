package org.logdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class BenchmarkUtils
{
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
