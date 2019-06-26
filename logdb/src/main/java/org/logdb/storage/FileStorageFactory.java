package org.logdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.Objects;

import static org.logdb.Config.LOG_DB_VERSION;

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

        //TODO: extract file creation and resize into a fileFactory
        final FileAllocator fileAllocator = FileAllocator.createNew(rootDirectory, fileType);
        final File newFile = fileAllocator.generateNextFile();
        if (!newFile.createNewFile())
        {
            throw new FileAlreadyExistsException("File " + newFile.getAbsolutePath() + " already exists");
        }

        FileStorage fileStorage = null;
        try
        {
            final RandomAccessFile dbFile = new RandomAccessFile(newFile, "rw");
            final FileChannel channel = dbFile.getChannel();
            dbFile.setLength(segmentFileSize);

            final FileDbHeader fileDbHeader = new FileDbHeader(
                    byteOrder,
                    LOG_DB_VERSION,
                    pageSizeBytes,
                    segmentFileSize,
                    StorageUnits.INVALID_OFFSET,
                    StorageUnits.INVALID_OFFSET
            );

            fileDbHeader.writeTo(channel);
            fileDbHeader.alignChannelToHeaderPage(channel);

            fileStorage = new FileStorage(
                    rootDirectory,
                    fileAllocator,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initFromFile();
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
            final FileAllocator fileAllocator = FileAllocator.openLatest(
                    rootDirectory,
                    lastFile,
                    fileType);

            final RandomAccessFile dbFile = new RandomAccessFile(lastFile.toFile(), "rw");
            final FileChannel channel = dbFile.getChannel();

            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(channel);
            fileDbHeader.alignChannelToHeaderPage(channel);

            fileStorage = new FileStorage(
                    rootDirectory,
                    fileAllocator,
                    fileDbHeader,
                    dbFile,
                    channel);

            fileStorage.initFromFile();
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
}
