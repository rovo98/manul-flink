package com.rovo98.manul.flink.connector.jdbc;

import com.rovo98.manul.flink.connector.jdbc.internal.options.ManulJdbcConnectionOptions;
import com.rovo98.manul.flink.connector.jdbc.split.JdbcSourceSplit;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Factory style interface to create bounded/unbounded jdbc Sources. */
public class JdbcSource<T> implements Source<T, JdbcSourceSplit, Collection<JdbcSourceSplit>> {
    private static final long serialVersionUID = 1L;

    private final String query;
    private final JdbcParameterValuesProvider parameterValuesProvider;
    private final ManulJdbcConnectionOptions jdbcConnectionOptions;
    private final ResultSetValueExtractor<T> resultSetValueExtractor;
    private final Boundedness boundedness;
    private int numSplits;

    private JdbcSource(
            String query,
            JdbcParameterValuesProvider parameterValuesProvider,
            ResultSetValueExtractor<T> resultSetValueExtractor,
            ManulJdbcConnectionOptions connectionOptions,
            Boundedness boundedness) {
        this.query = query;
        this.parameterValuesProvider = parameterValuesProvider;
        this.resultSetValueExtractor = resultSetValueExtractor;
        this.jdbcConnectionOptions = connectionOptions;
        this.boundedness = boundedness;
        if (this.parameterValuesProvider != null) {
            this.numSplits =
                    parameterValuesProvider.getParameterValues().length > 0
                            ? parameterValuesProvider.getParameterValues().length
                            : 1;
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<T, JdbcSourceSplit> createReader(SourceReaderContext sourceReaderContext)
            throws Exception {
        Configuration config = sourceReaderContext.getConfiguration();
        return new JdbcSourceReader<>(
                new FutureCompletingBlockingQueue<>(
                        config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)),
                () ->
                        new JdbcSourceSplitReader<>(
                                parameterValuesProvider,
                                jdbcConnectionOptions,
                                resultSetValueExtractor),
                new JdbcSourceRecordEmitter<>(),
                config,
                sourceReaderContext);
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, Collection<JdbcSourceSplit>> createEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> splitEnumeratorContext) throws Exception {
        List<JdbcSourceSplit> splits = new ArrayList<>();
        if (numSplits == 0) { // for the query that not needed to be set parameters
            splits.add(new JdbcSourceSplit(query, "" + (-1)));
        } else {
            for (int i = 0; i < numSplits; i++) {
                splits.add(new JdbcSourceSplit(query, "" + i));
            }
        }
        return new JdbcSourceSplitEnumerator(
                splitEnumeratorContext, splits, getBoundedness() == Boundedness.BOUNDED);
    }

    @Override
    public SplitEnumerator<JdbcSourceSplit, Collection<JdbcSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> splitEnumeratorContext,
            Collection<JdbcSourceSplit> checkpoint)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                splitEnumeratorContext, checkpoint, getBoundedness() == Boundedness.BOUNDED);
    }

    @Override
    public SimpleVersionedSerializer<JdbcSourceSplit> getSplitSerializer() {
        return new JdbcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<JdbcSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        return new JdbcEnumeratorCheckpointSerializer();
    }

    /**
     * Factory method to create a JdbcSource.
     *
     * @param query A string query which can contains parameter placeholder {@code ?}
     * @param parameterValuesProvider This provides the values for the parameters in the query.
     * @param resultSetValueExtractor To extract object of target type from the given {@link
     *     java.sql.ResultSet}.
     * @param connectionOptions options to build jdbc connection
     * @param bounded specify whether this source is bounded or unbounded.
     * @return Created {@link JdbcSource} instance
     * @param <T> the type of the query result.
     */
    public static <T> JdbcSource<T> source(
            String query,
            JdbcParameterValuesProvider parameterValuesProvider,
            ResultSetValueExtractor<T> resultSetValueExtractor,
            ManulJdbcConnectionOptions connectionOptions,
            boolean bounded) {
        // validate connectionOptions
        validateOptions(connectionOptions);
        return new JdbcSource<>(
                query,
                parameterValuesProvider,
                resultSetValueExtractor,
                connectionOptions,
                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
    }

    /**
     * Factory method to create a bounded JdbcSource.
     *
     * @param query A string query which can contains parameter placeholder {@code ?}
     * @param parameterValuesProvider This provides the values for the parameters in the query.
     * @param resultSetValueExtractor To extract object of target type from the given {@link
     *     java.sql.ResultSet}.
     * @param connectionOptions options to build jdbc connection
     * @return Created {@link JdbcSource} instance
     * @param <T> the type of the query result.
     */
    public static <T> JdbcSource<T> source(
            String query,
            JdbcParameterValuesProvider parameterValuesProvider,
            ResultSetValueExtractor<T> resultSetValueExtractor,
            ManulJdbcConnectionOptions connectionOptions) {
        return source(
                query, parameterValuesProvider, resultSetValueExtractor, connectionOptions, true);
    }

    private static void validateOptions(ManulJdbcConnectionOptions options) {
        Preconditions.checkNotNull(options.getDbURL());
        Preconditions.checkNotNull(options.getDriverName());
    }
}
