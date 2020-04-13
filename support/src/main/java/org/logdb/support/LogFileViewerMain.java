package org.logdb.support;

import org.logdb.logfile.LogRecordHeader;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.header.FileStorageDynamicHeader;
import org.logdb.storage.file.header.FileStorageHeader;
import org.logdb.storage.file.header.FileStorageStaticHeader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static java.lang.System.exit;

public class LogFileViewerMain
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 1)
        {
            System.out.println("Usage : <log file path>");
            return;
        }

        final String logFilePath = args[0];

        final File file = new File(logFilePath);

        if (!file.exists() || !file.isFile())
        {
            System.out.println("File " + logFilePath + " doesn't exists or is not a file");
            return;
        }

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel())
        {
            final FileStorageHeader fileHeader = FileStorageHeader.readFrom(fileChannel);

            final ByteOrder fileByteOrder = fileHeader.getOrder();
            final @ByteSize int pageSize = fileHeader.getPageSize();
            final @ByteOffset long lastPersistedOffset = fileHeader.getGlobalAppendOffset();
            final @ByteSize int checksumSize = fileHeader.getChecksumSize();

            System.out.println(
                    String.format("Log file header: \n\tpage Size %d \n\tlastPersistedOffset %d \n\tByte Order %s",
                            pageSize,
                            lastPersistedOffset,
                            fileByteOrder.toString()));

            final LogRecordHeader logRecordHeader = new LogRecordHeader(checksumSize);
            final ByteBuffer headerBuffer = ByteBuffer.allocate(logRecordHeader.getSize());

            headerBuffer.order(fileByteOrder);

            final @ByteOffset long fileHeaderOffsetToSkip = StorageUnits.offset(
                    FileStorageStaticHeader.getStaticHeaderSizeAlignedToNearestPage(pageSize) +
                    (FileStorageDynamicHeader.getDynamicHeaderSizeAlignedToNearestPage(pageSize) * 2));
            fileChannel.position(fileHeaderOffsetToSkip);

            long lastRecordSize = 0;
            for (long i = fileHeaderOffsetToSkip; i <= lastPersistedOffset; i += lastRecordSize)
            {
                headerBuffer.rewind();

                fileChannel.read(headerBuffer);

                //read header
                try
                {
                    logRecordHeader.read(headerBuffer);

                }
                catch (final Exception e)
                {
                    final @ByteOffset long originalOffset = StorageUnits.offset(fileChannel.position() - logRecordHeader.getSize());
                    System.out.println(
                            "Unable to parse header at offset " + originalOffset);
                    System.out.println("Header contents : " + new String(headerBuffer.array()));
                    e.printStackTrace();

                    exit(1);
                }

                //read key
                final ByteBuffer keyBuffer = ByteBuffer.allocate(logRecordHeader.getKeyLength());
                keyBuffer.order(fileByteOrder);
                fileChannel.read(keyBuffer);
                final String key = new String(keyBuffer.array());

                //read value
                final ByteBuffer valueBuffer = ByteBuffer.allocate(logRecordHeader.getValueLength());
                valueBuffer.order(fileByteOrder);
                fileChannel.read(valueBuffer);
                final String valueString = new String(valueBuffer.array());

                lastRecordSize = logRecordHeader.getSize() + logRecordHeader.getKeyLength() + logRecordHeader.getValueLength();

                switch (logRecordHeader.getRecordType())
                {
                    case UPDATE:
                        System.out.println(
                                String.format("offset %d : %s | key %s | value %s",
                                        i,
                                        logRecordHeader,
                                        key,
                                        valueString)
                        );
                        break;
                    case DELETE:
                        System.out.println(String.format("offset %d : %s | key %s ", i, logRecordHeader, key));
                        break;
                    default:
                        throw new RuntimeException("Unexpected record type " + logRecordHeader);
                }
            }
        }
    }
}
