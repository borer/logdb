package org.logdb.storage.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FixedFileStorageHeaderTest
{
    public static final String FIXED_HEADER_LOGINDEX = "fixed_header.logindex";
    @TempDir
    Path tempDirectory;

    @Test
    void shouldPersistHeaderOnlyInSetFile() throws Exception
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final FileStorageHeader expectedHeader = FileStorageHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageSizeBytes << 5);

        final File emptyFile = tempDirectory.resolve("empty_file.logindex").toFile();
        try (RandomAccessFile emptyRaf = new RandomAccessFile(emptyFile, "rw"))
        {
            try (FileChannel emptyChannel = emptyRaf.getChannel())
            {
                final File headerFile = tempDirectory.resolve(FIXED_HEADER_LOGINDEX).toFile();
                final RandomAccessFile headerRandomAccessFile = new RandomAccessFile(headerFile, "rw");
                final FileChannel headerFileChannel = headerRandomAccessFile.getChannel();
                try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                        expectedHeader,
                        headerRandomAccessFile,
                        headerFileChannel))
                {

                    fixedFileStorageHeader.writeToAndPageAlign(emptyChannel);
                    fixedFileStorageHeader.flush(true);

                    assertEquals(StorageUnits.ZERO_OFFSET, emptyChannel.position());
                    assertEquals(fixedFileStorageHeader.getHeaderSizeAlignedToNearestPage(), headerFileChannel.position());
                }
            }
        }
    }

    @Test
    void shouldPersistHeaderAndReadInSetFile() throws Exception
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final FileStorageHeader expectedHeader = FileStorageHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageSizeBytes << 5);

        final File headerFile = tempDirectory.resolve(FIXED_HEADER_LOGINDEX).toFile();
        final RandomAccessFile headerRandomAccessFile = new RandomAccessFile(headerFile, "rw");
        final FileChannel headerFileChannel = headerRandomAccessFile.getChannel();
        try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                expectedHeader,
                headerRandomAccessFile,
                headerFileChannel))
        {
            fixedFileStorageHeader.writeToAndPageAlign(null);
            fixedFileStorageHeader.flush(true);

            assertEquals(fixedFileStorageHeader.getHeaderSizeAlignedToNearestPage(), headerFileChannel.position());
        }


        final RandomAccessFile headerRandomAccessFile2 = new RandomAccessFile(headerFile, "rw");
        final FileChannel headerFileChannel2 = headerRandomAccessFile2.getChannel();
        final FileStorageHeader actualHeader = FileStorageHeader.readFrom(headerFileChannel2);

        try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                actualHeader,
                headerRandomAccessFile2,
                headerFileChannel2))
        {
            assertEquals(actualHeader.getPageSize(), fixedFileStorageHeader.getPageSize());
            assertEquals(actualHeader.getSegmentFileSize(), fixedFileStorageHeader.getSegmentFileSize());
            assertEquals(actualHeader.getOrder(), fixedFileStorageHeader.getOrder());
            assertEquals(actualHeader.getDbVersion(), fixedFileStorageHeader.getDbVersion());
            assertEquals(actualHeader.getAppendVersion(), fixedFileStorageHeader.getAppendVersion());
            assertEquals(actualHeader.getGlobalAppendOffset(), fixedFileStorageHeader.getGlobalAppendOffset());
            assertEquals(actualHeader.getLastFileAppendOffset(), fixedFileStorageHeader.getLastFileAppendOffset());
        }
    }
}