package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

public class JdbcSourceReader<T> extends SourceReaderBase<T, T, JdbcSourceSplit, JdbcSourceSplit> {

    public JdbcSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
            Supplier<SplitReader<T, JdbcSourceSplit>> splitReaderSupplier,
            RecordEmitter<T, T, JdbcSourceSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier),
                recordEmitter,
                config,
                context);
    }

    @Override
    public void start() {
        if (this.getNumberOfCurrentlyAssignedSplits() == 0) {
            this.context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, JdbcSourceSplit> finishedSplitIds) {
        this.context.sendSplitRequest();
    }

    @Override
    protected JdbcSourceSplit initializedState(JdbcSourceSplit split) {
        return split;
    }

    @Override
    protected JdbcSourceSplit toSplitType(String splitId, JdbcSourceSplit splitState) {
        return splitState;
    }
}
