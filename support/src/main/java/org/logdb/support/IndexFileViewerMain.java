package org.logdb.support;

import org.logdb.bbtree.BTreeMappedNode;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.header.FileHeader;
import org.logdb.storage.file.header.FileStorageDynamicHeader;
import org.logdb.storage.file.header.FileStorageHeader;
import org.logdb.storage.file.header.FileStorageStaticHeader;

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
            final FileHeader fileHeader = FileStorageHeader.readFrom(fileChannel);

            final ByteOrder fileByteOrder = fileHeader.getOrder();
            final @ByteSize int pageSize = fileHeader.getPageSize();
            final @ByteSize int pageLogSize = fileHeader.getPageLogSize();
            final @ByteOffset long lastPersistedOffset = fileHeader.getGlobalAppendOffset();

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
                    FileStorageStaticHeader.getStaticHeaderSizeAlignedToNearestPage(pageSize) +
                    (FileStorageDynamicHeader.getDynamicHeaderSizeAlignedToNearestPage(pageSize) * 2));
            final @PageNumber long headerPagesToSkip = StorageUnits.pageNumber(fileHeaderOffsetToSkip / pageSize);
            final @PageNumber long lastPersistedPageNumber = StorageUnits.pageNumber(fileHeader.getCurrentFileAppendOffset() / pageSize);

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
