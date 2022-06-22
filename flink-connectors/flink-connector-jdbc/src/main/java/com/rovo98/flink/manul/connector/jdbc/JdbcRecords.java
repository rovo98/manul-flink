package com.rovo98.flink.manul.connector.jdbc;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Payload for the elements passed from fetchers to {@link JdbcSourceSplitReader}.
 *
 * @param <T> the type of the record
 */
public final class JdbcRecords<T> implements RecordsWithSplitIds<T> {
    @Nullable private String splitId;

    @Nullable private Iterator<T> recordsForSplitCurrent;

    @Nullable private final Iterator<T> recordsForSplit;

    private final Set<String> finishedSplits;

    public JdbcRecords(
            @Nullable String splitId,
            @Nullable Iterator<T> recordsForSplit,
            Set<String> finishedSplits) {

        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSplits = finishedSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

        return nextSplit;
    }

    @Nullable
    @Override
    public T nextRecordFromSplit() {
        final Iterator<T> recordsForSplit = this.recordsForSplitCurrent;
        if (recordsForSplit != null && recordsForSplit.hasNext()) {
            return recordsForSplit.next();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    // -------------------------------------------------------------
    public static <T> JdbcRecords<T> forRecords(String splitId, Iterator<T> recordsForSplit) {
        return new JdbcRecords<>(splitId, recordsForSplit, Collections.emptySet());
    }

    public static <T> JdbcRecords<T> finishedSplit(String splitId) {
        return new JdbcRecords<>(null, null, Collections.singleton(splitId));
    }
}
