package org.logdb.storage.file;

import org.logdb.bit.UnsafeArrayList;
import org.logdb.checksum.Checksum;
import org.logdb.checksum.ChecksumFactory;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.logdb.storage.StorageUnits.INVALID_OFFSET;

public class FileStorageFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageFactory.class);
    private static final String ROOT_INDEX_HEADER_FILENAME = "root_index_header.logdb";

    public static FileStorage createNew(
            final Path rootDirectory,
            final FileType fileType,
            final @ByteSize long segmentFileSize,
            final ByteOrder byteOrder,
            final @ByteSize int pageSizeBytes,
            final ChecksumType checksumType) throws IOException
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

            final Checksum checksum = ChecksumFactory.checksumFromType(checksumType);
            final ChecksumHelper checksumHelper = new ChecksumHelper(checksum, checksumType);

            FileHeader fileHeader = FileStorageHeader.newHeader(byteOrder, pageSizeBytes, segmentFileSize, checksumHelper);
            FileHeader newFileHeader = FileStorageHeader.newHeader(
                    fileHeader.getOrder(),
                    fileHeader.getPageSize(),
                    fileHeader.getSegmentFileSize(),
                    checksumHelper
            );

            if (FileType.ROOT_INDEX == fileType)
            {
                final File rootIndexHeaderFile = rootDirectory.resolve(ROOT_INDEX_HEADER_FILENAME).toFile();
                final RandomAccessFile headerAccessFile = new RandomAccessFile(rootIndexHeaderFile, "rw");
                final FileChannel headerChannel = headerAccessFile.getChannel();
                fileHeader = new FixedFileStorageHeader(fileHeader, headerAccessFile, headerChannel);
                newFileHeader = new FixedFileStorageHeader(newFileHeader, headerAccessFile, headerChannel);
            }

            fileHeader.writeHeadersAndAlign(currentAppendChannel);

            fileStorage = createFileStorage(
                    rootDirectory,
                    fileAllocator,
                    fileHeader,
                    newFileHeader,
                    currentAppendFile,
                    currentAppendChannel
            );
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

    public static FileStorage openExisting(final Path rootDirectory, final FileType fileType, final ChecksumType checksumType)
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

            final Checksum checksum = ChecksumFactory.checksumFromType(checksumType);
            final ChecksumHelper checksumHelper = new ChecksumHelper(checksum, checksumType);

            final FileHeader fileHeader;
            final FileHeader newFileHeader;
            if (FileType.ROOT_INDEX == fileType)
            {
                final File rootIndexHeaderFile = rootDirectory.resolve(ROOT_INDEX_HEADER_FILENAME).toFile();
                final RandomAccessFile headerAccessFile = new RandomAccessFile(rootIndexHeaderFile, "rw");
                final FileChannel headerChannel = headerAccessFile.getChannel();

                final FileStorageHeader rootIndexHeader = FileStorageHeader.readFrom(headerChannel);
                fileHeader = new FixedFileStorageHeader(rootIndexHeader, headerAccessFile, headerChannel);
                newFileHeader = new FixedFileStorageHeader(
                        FileStorageHeader.newHeader(
                                rootIndexHeader.getOrder(),
                                rootIndexHeader.getPageSize(),
                                rootIndexHeader.getSegmentFileSize(),
                                checksumHelper),
                        headerAccessFile,
                        headerChannel);
            }
            else
            {
                fileHeader = FileStorageHeader.readFrom(currentAppendChannel);
                newFileHeader = FileStorageHeader.newHeader(
                        fileHeader.getOrder(),
                        fileHeader.getPageSize(),
                        fileHeader.getSegmentFileSize(),
                        checksumHelper);
            }

            fileStorage = createFileStorage(
                    rootDirectory,
                    fileAllocator,
                    fileHeader,
                    newFileHeader,
                    currentAppendFile,
                    currentAppendChannel
            );
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
            final FileHeader fileHeader,
            final FileHeader newFileHeader,
            final RandomAccessFile currentAppendFile,
            final FileChannel currentAppendChannel) throws IOException
    {
        final List<Path> existingFiles = fileAllocator.getAllFilesInOrder();
        final MappedBuffer[] mappedByteBuffers = new MappedBuffer[existingFiles.size()];
        for (int i = 0; i < existingFiles.size(); i++)
        {
            final Path filePath = existingFiles.get(i);
            LOGGER.info("Mapping file " + filePath);
            final File file = filePath.toFile();
            try (RandomAccessFile accessFile = new RandomAccessFile(file, "r"))
            {
                try (FileChannel channel = accessFile.getChannel())
                {
                    mappedByteBuffers[i] = FileStorage.mapFile(channel, fileHeader.getOrder());
                }
            }
        }

        final @ByteOffset long appendOffset = fileHeader.getCurrentFileAppendOffset();
        if (appendOffset != INVALID_OFFSET)
        {
            currentAppendChannel.position(appendOffset);
        }

        final @ByteOffset long globalFilePosition;
        if (appendOffset != INVALID_OFFSET && !existingFiles.isEmpty())
        {
            final long offset = ((existingFiles.size() - 1) * fileHeader.getSegmentFileSize()) + appendOffset;
            globalFilePosition = StorageUnits.offset(offset);
        }
        else
        {
            globalFilePosition = StorageUnits.offset(currentAppendChannel.position());
        }

        final UnsafeArrayList<MappedBuffer> mappedBuffers = new UnsafeArrayList<>(mappedByteBuffers);

        return new FileStorage(
                rootDirectory,
                fileAllocator,
                fileHeader,
                newFileHeader,
                currentAppendFile,
                currentAppendChannel,
                mappedBuffers,
                globalFilePosition);
    }

}
