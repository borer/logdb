package org.logdb.benchmark;

import org.logdb.bbtree.BTreeWithLog;
import org.logdb.storage.FileStorage;
import org.logdb.storage.NodesManager;
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

import java.io.File;
import java.nio.ByteOrder;
import java.util.Random;

public class TestRandomKeysWritingWithLogBenchmark
{
    private static final long MAPPED_CHUNK_SIZE = DefaultBenchmarkConfig.PAGE_SIZE_BYTES * 200;

    @State(Scope.Thread)
    public static class BenchmarkState
    {
        private File dbFile;
        private FileStorage storage;
        private NodesManager nodesManager;
        private BTreeWithLog btree;
        private Random random;

        @Setup(Level.Trial)
        public void doSetup()
        {
            dbFile = new File("benchmark.logdb");
            dbFile.delete();

            storage = FileStorage.createNewFileDb(
                    dbFile,
                    MAPPED_CHUNK_SIZE,
                    ByteOrder.LITTLE_ENDIAN,
                    DefaultBenchmarkConfig.PAGE_SIZE_BYTES);

            nodesManager = new NodesManager(storage);
            btree = new BTreeWithLog(nodesManager, new SystemTimeSource());
            random = new Random();
        }

        @TearDown(Level.Trial)
        public void doTearDown()
        {
            btree.close();
            dbFile.delete();
        }

        void putKeyValue()
        {
            btree.put(random.nextLong(), random.nextLong());
        }

        void commit()
        {
            btree.commit();
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
