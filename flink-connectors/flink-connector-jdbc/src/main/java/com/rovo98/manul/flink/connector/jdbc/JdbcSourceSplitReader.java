package com.rovo98.manul.flink.connector.jdbc;

import com.rovo98.manul.flink.connector.jdbc.internal.options.ManulJdbcConnectionOptions;
import com.rovo98.manul.flink.connector.jdbc.split.JdbcSourceSplit;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jdbc split reader used to read records from the given {@link JdbcSourceSplit}.
 *
 * @param <T> the final type of the record to emit
 * @param <SplitT> the type of the split
 */
public class JdbcSourceSplitReader<T, SplitT extends JdbcSourceSplit>
        implements SplitReader<T, SplitT> {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitReader.class);

    private final Queue<SplitT> splits;

    private ManulJdbcInputFormat<T> currentReader;
    private String currentSplitId;
    private final JdbcParameterValuesProvider parameterValuesProvider;
    private final ManulJdbcConnectionOptions connectionOptions;
    private final ResultSetValueExtractor<T> resultSetValueExtractor;

    private transient boolean noMoreSplits = false;

    public JdbcSourceSplitReader(
            JdbcParameterValuesProvider parameterValuesProvider,
            ManulJdbcConnectionOptions connectionOptions,
            ResultSetValueExtractor<T> resultSetValueExtractor) {
        this.parameterValuesProvider = parameterValuesProvider;
        this.connectionOptions = connectionOptions;
        this.resultSetValueExtractor = resultSetValueExtractor;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<T> fetch() throws IOException {
        checkSplitOrStartNext();
        if (noMoreSplits) {
            return finishSplit();
        }
        currentReader.openInputFormat();
        Iterator<T> resultItr = currentReader.readSplit();
        return resultItr == null
                ? finishSplit()
                : JdbcRecords.forRecords(currentSplitId, resultItr);
    }

    @Override
    public void handleSplitsChanges(SplitsChange splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        LOG.debug("Handling split change {}", splitsChange);
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            currentReader.closeInputFormat();
        }
    }

    private void checkSplitOrStartNext() {
        final SplitT nextSplit = splits.poll();
        if (nextSplit == null) {
            noMoreSplits = true;
            return;
        }

        currentSplitId = nextSplit.splitId();
        if (currentReader == null) {
            currentReader =
                    ManulJdbcInputFormat.<T>builder()
                            .setQuery(nextSplit.getQuery())
                            .setDBUrl(connectionOptions.getDbURL())
                            .setDriverName(connectionOptions.getDriverName())
                            .setUsername(connectionOptions.getUsername().orElse(null))
                            .setPassword(connectionOptions.getPassword().orElse(null))
                            .setFetchSize(connectionOptions.getFetchSize())
                            .setResultSetValueExtractor(resultSetValueExtractor)
                            .setParameterValues(
                                    parameterValuesProvider == null
                                            ? null
                                            : parameterValuesProvider
                                                    .getParameterValues()[
                                                    Integer.parseInt(currentSplitId)])
                            .build();
        } else {
            currentReader.setQueryParameterValues(
                    parameterValuesProvider == null
                            ? null
                            : parameterValuesProvider
                                    .getParameterValues()[Integer.parseInt(currentSplitId)]);
        }
    }

    private JdbcRecords<T> finishSplit() {
        final JdbcRecords<T> finishRecords = JdbcRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }
}
