package org.logdb.support;

import org.logdb.bbtree.BTreeMappedNode;
import org.logdb.bit.DirectMemory;
import org.logdb.bit.HeapMemory;
import org.logdb.bit.MemoryFactory;
import org.logdb.storage.ByteOffset;
import org.logdb.storage.ByteSize;
import org.logdb.storage.PageNumber;
import org.logdb.storage.Storage;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.Version;
import org.logdb.storage.file.FileDbHeader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
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
            final FileDbHeader fileDbHeader = FileDbHeader.readFrom(fileChannel);

            final ByteOrder fileByteOrder = fileDbHeader.byteOrder;
            final @ByteSize int pageSize = fileDbHeader.pageSize;
            final @ByteOffset long lastPersistedOffset = fileDbHeader.getGlobalAppendOffset();

            System.out.println(
                    String.format("Log file header: \n\tpage Size %d \n\tlastPersistedOffset %d \n\tByte Order %s",
                            pageSize,
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

            final MyStorage storage = new MyStorage(fileChannel, pageSize, fileByteOrder);
            storage.mapFile();

            final BTreeMappedNode bTreeMappedNode = new BTreeMappedNode(
                    (mappedNode) ->
                    {
                    },
                    storage,
                    storage.getUninitiatedDirectMemoryPage(),
                    StorageUnits.INVALID_PAGE_NUMBER);

            final @PageNumber long headerPagesToSkip = StorageUnits.pageNumber(fileDbHeader.getHeaderSizeAlignedToNearestPage() / pageSize);
            final @PageNumber long lastPersistedPageNumber = StorageUnits.pageNumber(fileDbHeader.getLastFileAppendOffset() / pageSize);

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

    private static final class MyStorage implements Storage
    {
        private final FileChannel fileChannel;
        private final @ByteSize int pageSize;
        private final ByteOrder fileByteOrder;
        private MappedByteBuffer mappedByteBuffer;

        MyStorage(final FileChannel fileChannel, final @ByteSize int pageSize, final ByteOrder fileByteOrder)
        {
            this.fileChannel = fileChannel;
            this.pageSize = pageSize;
            this.fileByteOrder = fileByteOrder;
        }

        void mapFile() throws IOException
        {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        }

        @Override
        public void close()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public HeapMemory allocateHeapPage()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public DirectMemory getUninitiatedDirectMemoryPage()
        {
            return MemoryFactory.getUninitiatedDirectMemory(pageSize, fileByteOrder);
        }

        @Override
        public @ByteSize long getPageSize()
        {
            return pageSize;
        }

        @Override
        public ByteOrder getOrder()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public @PageNumber long getPageNumber(final @ByteOffset long offset)
        {
            return StorageUnits.pageNumber(offset / pageSize);
        }

        @Override
        public @ByteOffset long getOffset(final @PageNumber long pageNumber)
        {
            return StorageUnits.offset(pageNumber * pageSize);
        }

        @Override
        public @ByteOffset long append(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public @PageNumber long appendPageAligned(ByteBuffer buffer)
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public void flush()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public void commitMetadata(@ByteOffset long lastPersistedOffset, @Version long version)
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public @ByteOffset long getLastPersistedOffset()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public @Version long getAppendVersion()
        {
            throw new UnsupportedOperationException("Method not Implemented");
        }

        @Override
        public void mapPage(final @PageNumber long pageNumber, final DirectMemory memory)
        {
            assert pageNumber > 0 : "page number must be > 0, provided " + pageNumber;

            final @ByteOffset long pageOffset = MemoryFactory.getPageOffset(mappedByteBuffer, getOffset(pageNumber));
            memory.setBaseAddress(pageOffset);
        }
    }
}
