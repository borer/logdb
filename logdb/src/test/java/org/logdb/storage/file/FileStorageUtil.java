package org.logdb.storage.file;

import org.logdb.storage.Version;
import org.logdb.storage.file.header.FileHeader;
import org.logdb.storage.file.header.FileStorageHeader;

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
            final FileHeader fileStorageHeader = FileStorageHeader.readFrom(fileChannel);
            return fileStorageHeader.getAppendVersion();
        }
    }
}
