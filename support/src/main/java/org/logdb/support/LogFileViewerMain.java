package org.logdb.support;

import org.logdb.logfile.LogRecordHeader;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorageHeader;

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
            final FileStorageHeader fileStorageHeader = FileStorageHeader.readFrom(fileChannel);

            final ByteOrder fileByteOrder = fileStorageHeader.getOrder();
            final @ByteSize int pageSize = fileStorageHeader.getPageSize();
            final @ByteOffset long lastPersistedOffset = fileStorageHeader.getGlobalAppendOffset();

            System.out.println(
                    String.format("Log file header: \n\tpage Size %d \n\tlastPersistedOffset %d \n\tByte Order %s",
                            pageSize,
                            lastPersistedOffset,
                            fileByteOrder.toString()));

            final LogRecordHeader logRecordHeader = new LogRecordHeader();
            final ByteBuffer headerBuffer = ByteBuffer.allocate(LogRecordHeader.RECORD_HEADER_SIZE);

            headerBuffer.order(fileByteOrder);

            final @ByteOffset long fileHeaderOffsetToSkip = StorageUnits.offset(
                    FileStorageHeader.getStaticHeaderSizeAlignedToNearestPage(pageSize) +
                    (FileStorageHeader.getDynamicHeaderSizeAlignedToNearestPage(pageSize) * 2));
            fileChannel.position(fileHeaderOffsetToSkip);

            long lastRecordSize = 0;
            final ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
            keyBuffer.order(fileByteOrder);
            for (long i = fileHeaderOffsetToSkip; i <= lastPersistedOffset; i += lastRecordSize)
            {
                headerBuffer.rewind();
                keyBuffer.rewind();

                fileChannel.read(headerBuffer);

                //read header
                try
                {
                    logRecordHeader.read(headerBuffer);
                }
                catch (final Exception e)
                {
                    final long originalOffset = fileChannel.position() - LogRecordHeader.RECORD_HEADER_SIZE;
                    System.out.println(
                            "Unable to parse header at offset " + originalOffset);
                    System.out.println("Header contents : " + new String(headerBuffer.array()));
                    e.printStackTrace();

                    exit(1);
                }

                //read key
                fileChannel.read(keyBuffer);
                keyBuffer.rewind();
                final long key = keyBuffer.getLong();

                //read value
                final ByteBuffer valueBuffer = ByteBuffer.allocate(logRecordHeader.getValueLength());
                valueBuffer.order(fileByteOrder);
                fileChannel.read(valueBuffer);
                final String valueString = new String(valueBuffer.array());

                lastRecordSize =
                        LogRecordHeader.RECORD_HEADER_SIZE +
                                logRecordHeader.getKeyLength() +
                                logRecordHeader.getValueLength();

                System.out.println(
                        String.format("offset %d : %s | key %d | value %s",
                                i,
                                logRecordHeader,
                                key,
                                valueString)
                );
            }
        }
    }
}
