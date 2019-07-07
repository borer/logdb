package org.logdb.storage.file;

import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.INVALID_OFFSET;

public class FileStorageFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageFactory.class);

    public static FileStorage createNew(
            final Path rootDirectory,
            final FileType fileType,
            final @ByteSize long segmentFileSize,
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes) throws IOException
    {
        Objects.requireNonNull(rootDirectory, "Database root directory cannot be null");

        if (pageSizeBytes < 128 || pageSizeBytes % 2 != 0)
        {
            throw new IllegalArgumentException("Page Size must be bigger than 128 bytes and a power of 2. Provided was " + pageSizeBytes);
        }

        if (segmentFileSize % pageSizeBytes != 0)
        {
            throw new IllegalArgumentException(
                    "Segment file size must be multiple of page size. Provided page size was " + pageSizeBytes +
                            " , provided segment file size was " + segmentFileSize);
        }

        LOGGER.info("Using database root directory " + rootDirectory.toAbsolutePath().toString());

        final FileAllocator fileAllocator = FileAllocator.createNew(rootDirectory, fileType);
        final File newFile = fileAllocator.generateNextFile();

        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile currentAppendFile = new RandomAccessFile(newFile, "rw");
            final FileChannel currentAppendChannel = currentAppendFile.getChannel();
            currentAppendFile.setLength(segmentFileSize);

            final FileStorageHeader fileStorageHeader = FileStorageHeader.newHeader(byteOrder, pageSizeBytes, segmentFileSize);
            fileStorageHeader.writeToAndPageAlign(currentAppendChannel);

            fileStorage = createFileStorage(rootDirectory, fileAllocator, fileStorageHeader, currentAppendFile, currentAppendChannel);
        }
        catch (final FileNotFoundException e)
        {
            LOGGER.error("Unable to find db file " + newFile.getAbsolutePath(), e);
        }
        catch (final IOException e)
        {
            LOGGER.error("Unable to read/write to db file " + newFile.getAbsolutePath(), e);
        }

        return fileStorage;
    }

    public static FileStorage openExisting(final Path rootDirectory, final FileType fileType)
    {
        Objects.requireNonNull(rootDirectory, "Database root directory cannot be null");

        LOGGER.info("Loading database from directory " + rootDirectory.toAbsolutePath().toString());

        FileStorage fileStorage = null;
        try
        {
            final Path lastFile = FileAllocator.findLastFile(rootDirectory, fileType);
            final FileAllocator fileAllocator = FileAllocator.openLatest(rootDirectory, lastFile, fileType);

            final RandomAccessFile currentAppendFile = new RandomAccessFile(lastFile.toFile(), "rw");
            final FileChannel currentAppendChannel = currentAppendFile.getChannel();

            final FileStorageHeader fileStorageHeader = FileStorageHeader.readFrom(currentAppendChannel);

            fileStorage = createFileStorage(rootDirectory, fileAllocator, fileStorageHeader, currentAppendFile, currentAppendChannel);
        }
        catch (final FileNotFoundException e)
        {
            LOGGER.error(String.format("Unable to find latest file type %s in %s",
                    fileType.name(),
                    rootDirectory.toAbsolutePath().toString()),
                    e);
        }
        catch (final IOException e)
        {
            LOGGER.error(String.format("Unable to open/list file type %s in %s",
                    fileType.name(),
                    rootDirectory.toAbsolutePath().toString()),
                    e);
        }

        return fileStorage;
    }

    private static FileStorage createFileStorage(
            final Path rootDirectory,
            final FileAllocator fileAllocator,
            final FileStorageHeader fileStorageHeader,
            final RandomAccessFile currentAppendFile,
            final FileChannel currentAppendChannel) throws IOException
    {
        final List<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();
        final List<Path> existingFiles = fileAllocator.getAllFilesInOrder();
        for (final Path filePath : existingFiles)
        {
            LOGGER.info("Mapping file " + filePath);
            final File file = filePath.toFile();
            try (RandomAccessFile accessFile = new RandomAccessFile(file, "r"))
            {
                try (FileChannel channel = accessFile.getChannel())
                {
                    mappedByteBuffers.add(FileStorage.mapFile(channel, fileStorageHeader.byteOrder));
                }
            }
        }

        final @ByteOffset long appendOffset = fileStorageHeader.getLastFileAppendOffset();
        if (appendOffset != INVALID_OFFSET)
        {
            currentAppendChannel.position(appendOffset);
        }

        final @ByteOffset long globalFilePosition;
        if (appendOffset != INVALID_OFFSET && !existingFiles.isEmpty())
        {
            globalFilePosition = StorageUnits.offset(((existingFiles.size() - 1) * fileStorageHeader.segmentFileSize) + appendOffset);
        }
        else
        {
            globalFilePosition = StorageUnits.offset(currentAppendChannel.position());
        }

        return new FileStorage(
                rootDirectory,
                fileAllocator,
                fileStorageHeader,
                currentAppendFile,
                currentAppendChannel,
                mappedByteBuffers,
                globalFilePosition);
    }

}
