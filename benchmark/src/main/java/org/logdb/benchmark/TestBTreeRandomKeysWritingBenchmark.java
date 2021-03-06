package org.logdb.benchmark;

import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.bbtree.NodesManager;
import org.logdb.bit.BinaryHelper;
import org.logdb.checksum.ChecksumType;
import org.logdb.root.index.RootIndex;
import org.logdb.storage.StorageUnits;
import org.logdb.storage.file.FileStorage;
import org.logdb.storage.file.FileStorageFactory;
import org.logdb.storage.file.FileType;
import org.logdb.time.SystemTimeSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.logdb.benchmark.BenchmarkUtils.createInitialRootReference;
import static org.logdb.benchmark.BenchmarkUtils.createRootIndex;
import static org.logdb.benchmark.DefaultBenchmarkConfig.INITIAL_VERSION;
import static org.logdb.benchmark.DefaultBenchmarkConfig.NODE_LOG_SIZE;
import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;
import static org.logdb.benchmark.DefaultBenchmarkConfig.SEGMENT_FILE_SIZE;

public class TestBTreeRandomKeysWritingBenchmark
{
    @State(Scope.Thread)
    public static class BenchmarkState
    {
        private Path rootDirectory;
        private FileStorage storage;
        private NodesManager nodesManager;
        private BTree btree;
        private Random random;
        private byte[] longBuffer;

        @Setup(Level.Trial)
        public void doSetup() throws IOException
        {
            rootDirectory = Paths.get("./");

            storage = FileStorageFactory.createNew(
                    rootDirectory,
                    FileType.INDEX,
                    SEGMENT_FILE_SIZE,
                    ByteOrder.LITTLE_ENDIAN,
                    DefaultBenchmarkConfig.PAGE_SIZE_BYTES,
                    DefaultBenchmarkConfig.PAGE_SIZE_BYTES,
                    ChecksumType.CRC32);

            final RootIndex rootIndex = createRootIndex(
                    rootDirectory,
                    SEGMENT_FILE_SIZE,
                    PAGE_SIZE_BYTES,
                    NODE_LOG_SIZE,
                    ByteOrder.LITTLE_ENDIAN);

            nodesManager = new NodesManager(storage, rootIndex, false, NODE_LOG_SIZE);

            btree = new BTreeImpl(
                    nodesManager,
                    new SystemTimeSource(),
                    INITIAL_VERSION,
                    StorageUnits.INVALID_PAGE_NUMBER,
                    createInitialRootReference(nodesManager));
            random = new Random();
            longBuffer = new byte[Long.BYTES];
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception
        {
            btree.close();
            BenchmarkUtils.removeAllFilesFromDirectory(rootDirectory);
        }

        void putKeyValue()
        {
            BinaryHelper.longToBytes(random.nextLong(), longBuffer);
            btree.put(longBuffer, longBuffer);
        }

        void commit()
        {
            try
            {
                btree.commit();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Threads(1)
    public void testBench(final BenchmarkState benchmarkState)
    {
        benchmarkState.putKeyValue();
        benchmarkState.commit();
    }
}
