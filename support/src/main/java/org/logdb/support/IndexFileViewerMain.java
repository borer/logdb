package org.logdb.support;

import org.logdb.bbtree.BTreeMappedNode;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorageHeader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class IndexFileViewerMain
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 1)
        {
            System.out.println("Usage : <index file path>");
            return;
        }

        final String indexFilePath = args[0];

        final File file = new File(indexFilePath);

        if (!file.exists() || !file.isFile())
        {
            System.out.println("File " + indexFilePath + " doesn't exists or is not a file");
            return;
        }

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel())
        {
            final FileStorageHeader fileStorageHeader = FileStorageHeader.readFrom(fileChannel);

            final ByteOrder fileByteOrder = fileStorageHeader.getOrder();
            final @ByteSize int pageSize = fileStorageHeader.getPageSize();
            final @ByteSize int pageLogSize = fileStorageHeader.getPageLogSize();
            final @ByteOffset long lastPersistedOffset = fileStorageHeader.getGlobalAppendOffset();

            System.out.println(
                    String.format("Log file header: \n\tpage Size %d \n\tpage log size %d\n\tlastPersistedOffset %d \n\tByte Order %s",
                            pageSize,
                            pageLogSize,
                            lastPersistedOffset,
                            fileByteOrder.toString()));

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

            final BTreeMappedNode bTreeMappedNode = new BTreeMappedNode(
                    (mappedNode) ->
                    {
                    },
                    storage,
                    storage.getUninitiatedDirectMemoryPage(),
                    StorageUnits.INVALID_PAGE_NUMBER,
                    pageLogSize);

            final @ByteOffset long fileHeaderOffsetToSkip = StorageUnits.offset(
                    FileStorageHeader.getStaticHeaderSizeAlignedToNearestPage(pageSize) +
                    (FileStorageHeader.getDynamicHeaderSizeAlignedToNearestPage(pageSize) * 2));
            final @PageNumber long headerPagesToSkip = StorageUnits.pageNumber(fileHeaderOffsetToSkip / pageSize);
            final @PageNumber long lastPersistedPageNumber = StorageUnits.pageNumber(fileStorageHeader.getCurrentFileAppendOffset() / pageSize);

            for (long i = headerPagesToSkip; i < lastPersistedPageNumber; i++)
            {
                bTreeMappedNode.initNode(StorageUnits.pageNumber(i));
                System.out.println(
                        String.format("page/offset %d/%d : %s",
                                i,
                                i * pageSize,
                                bTreeMappedNode.toString()));
            }
        }
    }
}
