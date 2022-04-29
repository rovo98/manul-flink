package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class FixedSizeJdbcSourceSplitFetcherManager<T>
        extends SplitFetcherManager<T, JdbcSourceSplit> {
    private final int numOfFetchers;

    public FixedSizeJdbcSourceSplitFetcherManager(
            int numOfFetchers,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
            Supplier<SplitReader<T, JdbcSourceSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
        this.numOfFetchers = numOfFetchers;
        // Create numFetchers split fetchers.
        for (int i = 0; i < numOfFetchers; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splitsToAdd) {
        // Group splits by their owner fetchers
        Map<Integer, List<JdbcSourceSplit>> splitsByFetcherIndex = new HashMap<>();
        splitsToAdd.forEach(
                split -> {
                    int ownerFetcherIndex = split.hashCode() % numOfFetchers;
                    splitsByFetcherIndex
                            .computeIfAbsent(ownerFetcherIndex, s -> new LinkedList<>())
                            .add(split);
                });
        // Assign the splits to their owner fetcher.
        splitsByFetcherIndex.forEach(
                (fetcherIndex, splitsForFetcher) -> {
                    fetchers.get(fetcherIndex).addSplits(splitsForFetcher);
                });
    }
}
