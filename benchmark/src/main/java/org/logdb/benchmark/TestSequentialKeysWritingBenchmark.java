package org.logdb.benchmark;

import org.logdb.bbtree.BTree;
import org.logdb.bbtree.BTreeImpl;
import org.logdb.storage.FileStorage;
import org.logdb.storage.NodesManager;
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

import static org.logdb.benchmark.DefaultBenchmarkConfig.PAGE_SIZE_BYTES;

public class TestSequentialKeysWritingBenchmark
{
    private static final long MAPPED_CHUNK_SIZE = PAGE_SIZE_BYTES * 200;

    @State(Scope.Thread)
    public static class BenchmarkState
    {
        private long key;
        private File dbFile;
        private FileStorage storage;
        private NodesManager nodesManager;
        private BTree btree;

        @Setup(Level.Trial)
        public void doSetup()
        {
            dbFile = new File("benchmark.logdb");
            dbFile.delete();

            storage = FileStorage.createNewFileDb(
                    dbFile,
                    MAPPED_CHUNK_SIZE,
                    ByteOrder.LITTLE_ENDIAN,
                    PAGE_SIZE_BYTES);

            nodesManager = new NodesManager(storage);

            btree = new BTreeImpl(nodesManager);

            key = 1L;
        }

        @TearDown(Level.Trial)
        public void doTearDown()
        {
            btree.close();
            dbFile.delete();
        }

        void putKey()
        {
            btree.put(key, key);
            key++;
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
        benchmarkState.putKey();
        benchmarkState.commit();
    }
}
