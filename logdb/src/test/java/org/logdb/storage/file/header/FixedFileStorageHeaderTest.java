package org.logdb.storage.file.header;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.logdb.checksum.ChecksumHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.checksum.Crc32;
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
    private static final String FIXED_HEADER_LOGINDEX = "fixed_header.logindex";
    private static final Crc32 CHECKSUM = new Crc32();
    private static final ChecksumHelper CHECKSUM_HELPER = new ChecksumHelper(CHECKSUM, ChecksumType.CRC32);

    @TempDir
    Path tempDirectory;

    @Test
    void shouldPersistHeaderOnlyInSetFile() throws Exception
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final @ByteSize int pageLogSize = StorageUnits.size(1228);
        final FileHeader fileHeader = FileStorageHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageLogSize,
                pageSizeBytes << 5,
                CHECKSUM_HELPER);


        final File emptyFile = tempDirectory.resolve("empty_file.logindex").toFile();
        try (RandomAccessFile emptyRaf = new RandomAccessFile(emptyFile, "rw"))
        {
            try (FileChannel emptyChannel = emptyRaf.getChannel())
            {
                final File headerFile = tempDirectory.resolve(FIXED_HEADER_LOGINDEX).toFile();
                final RandomAccessFile headerRandomAccessFile = new RandomAccessFile(headerFile, "rw");
                final FileChannel headerFileChannel = headerRandomAccessFile.getChannel();
                try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                        fileHeader,
                        headerRandomAccessFile,
                        headerFileChannel))
                {

                    fixedFileStorageHeader.writeAlign(emptyChannel);
                    fixedFileStorageHeader.flush(true);

                    assertEquals(StorageUnits.ZERO_OFFSET, emptyChannel.position());
                    assertEquals(pageSizeBytes * 3, headerFileChannel.position());
                }
            }
        }
    }

    @Test
    void shouldPersistHeaderAndReadInSetFile() throws Exception
    {
        final @ByteSize int pageSizeBytes = StorageUnits.size(4096);
        final @ByteSize int pageLogSize = StorageUnits.size(1228);

        final FileHeader delegateHeader = FileStorageHeader.newHeader(
                ByteOrder.BIG_ENDIAN,
                pageSizeBytes,
                pageLogSize,
                pageSizeBytes << 5,
                CHECKSUM_HELPER);

        final File headerFile = tempDirectory.resolve(FIXED_HEADER_LOGINDEX).toFile();
        final RandomAccessFile headerRandomAccessFile = new RandomAccessFile(headerFile, "rw");
        final FileChannel headerFileChannel = headerRandomAccessFile.getChannel();
        try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                delegateHeader,
                headerRandomAccessFile,
                headerFileChannel))
        {
            fixedFileStorageHeader.writeAlign(null);
            fixedFileStorageHeader.flush(true);

            assertEquals(pageSizeBytes * 3, headerFileChannel.position());
        }

        ////Read persisted header
        final RandomAccessFile headerRandomAccessFile2 = new RandomAccessFile(headerFile, "rw");
        final FileChannel headerFileChannel2 = headerRandomAccessFile2.getChannel();
        final FileHeader readDelegateHeader = FileStorageHeader.readFrom(headerFileChannel2);

        try (FixedFileStorageHeader fixedFileStorageHeader = new FixedFileStorageHeader(
                readDelegateHeader,
                headerRandomAccessFile2,
                headerFileChannel2))
        {
            assertEquals(delegateHeader.getPageSize(), fixedFileStorageHeader.getPageSize());
            assertEquals(delegateHeader.getSegmentFileSize(), fixedFileStorageHeader.getSegmentFileSize());
            assertEquals(delegateHeader.getOrder(), fixedFileStorageHeader.getOrder());
            assertEquals(delegateHeader.getDbVersion(), fixedFileStorageHeader.getDbVersion());
            assertEquals(delegateHeader.getAppendVersion(), fixedFileStorageHeader.getAppendVersion());
            assertEquals(delegateHeader.getGlobalAppendOffset(), fixedFileStorageHeader.getGlobalAppendOffset());
            assertEquals(delegateHeader.getCurrentFileAppendOffset(), fixedFileStorageHeader.getCurrentFileAppendOffset());
            assertEquals(delegateHeader.getChecksumSize(), fixedFileStorageHeader.getChecksumSize());
        }
    }
}