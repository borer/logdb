package org.borer.logdb.benchmark;

import org.borer.logdb.BTree;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

public class TestBenchmark
{
    @State(Scope.Thread)
    public static class BenchmarkState
    {
        long key = 1L;
        private final BTree btree = new BTree();

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
