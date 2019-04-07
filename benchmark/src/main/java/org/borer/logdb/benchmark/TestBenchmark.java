package org.borer.logdb.benchmark;

import org.borer.logdb.Config;
import org.borer.logdb.bbtree.BTree;
import org.borer.logdb.storage.MemoryStorage;
import org.borer.logdb.storage.NodesManager;
import org.borer.logdb.storage.Storage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import java.nio.ByteOrder;

public class TestBenchmark
{
    @State(Scope.Thread)
    public static class BenchmarkState
    {
        long key = 1L;
        final Storage storage = new MemoryStorage(ByteOrder.BIG_ENDIAN, Config.PAGE_SIZE_BYTES);
        final NodesManager nodesManager = new NodesManager(storage);
        private final BTree btree = new BTree(nodesManager);

        void putKey()
        {
            btree.put(key, key);
            key++;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Fork(0)
    @Threads(1)
    public void testBench(final BenchmarkState benchmarkState)
    {
        benchmarkState.putKey();
    }
}
