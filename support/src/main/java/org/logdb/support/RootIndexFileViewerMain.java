package org.logdb.support;

import org.logdb.root.index.RootIndexRecord;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorageHeader;
import org.logdb.time.TimeUnits;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class RootIndexFileViewerMain
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 2)
        {
            System.out.println("Usage : <root index header file> <root index file path>");
            return;
        }

        final String indexHeaderFilePath = args[0];

        final File headerFile = new File(indexHeaderFilePath);
        if (!headerFile.exists() || !headerFile.isFile())
        {
            System.out.println("File " + indexHeaderFilePath + " doesn't exists or is not a file");
            return;
        }

        ByteOrder fileByteOrder;
        @ByteSize int pageSize;
        @ByteOffset long lastPersistedOffset;
        try (FileChannel fileChannel = new RandomAccessFile(headerFile, "r").getChannel())
        {
            final FileStorageHeader fileStorageHeader = FileStorageHeader.readFrom(fileChannel);

            fileByteOrder = fileStorageHeader.getOrder();
            pageSize = fileStorageHeader.getPageSize();
            lastPersistedOffset = fileStorageHeader.getGlobalAppendOffset();

            System.out.println(
                    String.format("Log file header: \n\tpage Size %d \n\tlastPersistedOffset %d \n\tByte Order %s",
                            pageSize,
                            lastPersistedOffset,
                            fileByteOrder.toString()));
        }

        final String indexFilePath = args[1];
        final File file = new File(indexFilePath);
        if (!file.exists() || !file.isFile())
        {
            System.out.println("File " + indexFilePath + " doesn't exists or is not a file");
            return;
        }

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel())
        {
            if (file.length() % pageSize != 0)
            {
                System.out.println("===============Warning===============");
                System.out.println("The file size is " + file.length() +
                        " which is not aligned to file storage page size " + pageSize);
                System.out.println("Last page content will not be displayed");
                System.out.println("===============End Warning===============");
            }

            final FileReaderStorage storage = new FileReaderStorage(fileChannel, pageSize, fileByteOrder);
            storage.mapFile();

            final RootIndexRecord rootIndexRecord = new RootIndexRecord(
                    fileByteOrder,
                    StorageUnits.INITIAL_VERSION,
                    TimeUnits.millis(0),
                    StorageUnits.INVALID_OFFSET);

            for (long offset = 0; offset <= lastPersistedOffset; offset += RootIndexRecord.SIZE)
            {
                storage.readBytes(offset, rootIndexRecord.getBuffer());

                System.out.println(
                        String.format("page/offset %d/%d : %s",
                                offset,
                                offset * pageSize,
                                rootIndexRecord.toString()));
            }
        }
    }
}
