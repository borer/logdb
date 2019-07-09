package org.logdb.benchmark;

import org.logdb.bbtree.BTreeWithLog;
import org.logdb.bbtree.NodesManager;
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
import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;
import static org.logdb.benchmark.DefaultBenchmarkConfig.SEGMENT_FILE_SIZE;

public class TestRandomKeysWritingWithLogBenchmark
{


    @State(Scope.Thread)
    public static class BenchmarkState
    {
        private Path rootDirectory;
        private FileStorage storage;
        private NodesManager nodesManager;
        private BTreeWithLog btree;
        private Random random;

        @Setup(Level.Trial)
        public void doSetup() throws IOException
        {
            rootDirectory = Paths.get("./");

            storage = FileStorageFactory.createNew(
                    rootDirectory,
                    FileType.INDEX,
                    SEGMENT_FILE_SIZE,
                    ByteOrder.LITTLE_ENDIAN,
                    DefaultBenchmarkConfig.PAGE_SIZE_BYTES);

            final RootIndex rootIndex = createRootIndex(
                    rootDirectory,
                    SEGMENT_FILE_SIZE,
                    PAGE_SIZE_BYTES,
                    ByteOrder.LITTLE_ENDIAN);

            nodesManager = new NodesManager(storage, rootIndex);

            btree = new BTreeWithLog(
                    nodesManager,
                    rootIndex,
                    new SystemTimeSource(),
                    INITIAL_VERSION,
                    StorageUnits.INVALID_PAGE_NUMBER,
                    createInitialRootReference(nodesManager));
            random = new Random();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws Exception
        {
            btree.close();
            BenchmarkUtils.removeAllFilesFromDirectory(rootDirectory);
        }

        void putKeyValue()
        {
            btree.put(random.nextLong(), random.nextLong());
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
