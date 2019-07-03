package org.logdb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileStorageUtil
{
    public static @Version long getLastAppendVersion(
            final Path rootDirectory,
            final FileType fileType) throws IOException
    {
        final Path lastFile = FileAllocator.findLastFile(rootDirectory, fileType);

        try (FileChannel fileChannel = new RandomAccessFile(lastFile.toFile(), "r").getChannel())
        {
            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(fileChannel);

            return fileDbHeader.getAppendVersion();
        }
    }
}
