package org.logdb.support;

import org.logdb.bbtree.BTreeMappedNode;
import org.logdb.storage.FileStorage;
import org.logdb.storage.StorageUnits;

import java.io.File;
import java.io.IOException;

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

        final FileStorage fileStorage = FileStorage.openDbFile(file);

        if (file.length() % fileStorage.getPageSize() != 0)
        {
            System.out.println("===============Warning===============");
            System.out.println("The file size is " + file.length() + " which is not aligned to file storage page size " + fileStorage.getPageSize());
            System.out.println("Last page content will not be displayed");
            System.out.println("===============End Warning===============");
        }

        final long fileLengthInPages = file.length() / fileStorage.getPageSize();

        final BTreeMappedNode bTreeMappedNode = new BTreeMappedNode(
                null,
                fileStorage,
                fileStorage.getUninitiatedDirectMemoryPage(),
                fileStorage.getPageSize(),
                StorageUnits.INVALID_PAGE_NUMBER);

        final long headerPagesToSkip = 1;

        for (long i = headerPagesToSkip; i < fileLengthInPages; i++)
        {
            bTreeMappedNode.initNode(StorageUnits.pageNumber(i));
            System.out.println(
                    String.format("page/offset %d/%d : %s",
                            i,
                            i * fileStorage.getPageSize(),
                            bTreeMappedNode.toString()));
        }

        fileStorage.close();
    }
}
