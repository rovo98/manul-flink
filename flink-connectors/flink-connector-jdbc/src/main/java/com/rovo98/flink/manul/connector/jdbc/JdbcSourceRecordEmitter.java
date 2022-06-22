package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * @param <T> the type of the record to be emitted
 * @param <SplitT> the type of the SourceSplit
 */
public class JdbcSourceRecordEmitter<T, SplitT extends JdbcSourceSplit>
        implements RecordEmitter<T, T, SplitT> {
    @Override
    public void emitRecord(T element, SourceOutput<T> output, SplitT splitState) throws Exception {
        output.collect(element);
        // currently we do nothing with the splitState.
    }
}
