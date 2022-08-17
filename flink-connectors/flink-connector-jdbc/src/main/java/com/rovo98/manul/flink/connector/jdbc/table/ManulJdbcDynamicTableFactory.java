/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rovo98.manul.flink.connector.jdbc.table;

import com.rovo98.manul.flink.connector.jdbc.dialect.JdbcDialectService;
import com.rovo98.manul.flink.connector.jdbc.internal.options.ManulJdbcReadOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.DRIVER;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.PASSWORD;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_AUTO_COMMIT;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_FETCH_SIZE;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_COLUMN;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_COLUMN_STRING_TYPE;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_NUM;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_STRING_COLUMN_VALUES;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SCAN_PUSH_DOWN_CONSTRAINTS;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SINK_MAX_RETRIES;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.SINK_PARALLELISM;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.TABLE_NAME;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.URL;
import static com.rovo98.manul.flink.connector.jdbc.table.ManulJdbcConnectorOptions.USERNAME;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 *
 * <p>Migrated from flink, was modified to support more features.
 */
public class ManulJdbcDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "manul-jdbc";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        JdbcConnectorOptions jdbcOptions = getJdbcOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new JdbcDynamicTableSink(
                jdbcOptions,
                getJdbcExecutionOptions(config),
                getJdbcDmlOptions(jdbcOptions, physicalSchema),
                physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new ManulJdbcDynamicTableSource(
                getJdbcOptions(helper.getOptions()),
                getJdbcReadOptions(helper.getOptions()),
                getJdbcLookupOptions(helper.getOptions()),
                physicalSchema);
    }

    private JdbcConnectorOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcConnectorOptions.Builder builder =
                JdbcConnectorOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setDialect(JdbcDialectService.load(url))
                        .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
                        .setConnectionCheckTimeoutSeconds(
                                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private ManulJdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName =
                readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final ManulJdbcReadOptions.Builder builder = ManulJdbcReadOptions.builder();
        partitionColumnName.ifPresent(
                pcn -> {
                    builder.setPartitionColumnName(pcn);

                    readableConfig
                            .getOptional(SCAN_PARTITION_NUM)
                            .ifPresent(
                                    np ->
                                            builder.setNumPartitions(np)
                                                    .setPartitionLowerBound(
                                                            readableConfig.get(
                                                                    SCAN_PARTITION_LOWER_BOUND))
                                                    .setPartitionUpperBound(
                                                            readableConfig.get(
                                                                    SCAN_PARTITION_UPPER_BOUND)));
                    readableConfig
                            .getOptional(SCAN_PARTITION_COLUMN_STRING_TYPE)
                            .ifPresent(
                                    pst ->
                                            builder.setPartitionColumnStringType(pst)
                                                    .setStringPartitionColumnValues(
                                                            readableConfig.get(
                                                                    SCAN_PARTITION_STRING_COLUMN_VALUES)));
                });
        readableConfig.getOptional(SCAN_FETCH_SIZE).ifPresent(builder::setFetchSize);
        builder.setAutoCommit(readableConfig.get(SCAN_AUTO_COMMIT));
        readableConfig
                .getOptional(SCAN_PUSH_DOWN_CONSTRAINTS)
                .ifPresent(builder::setWhereClausePushDownConstraints);
        return builder.build();
    }

    private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig readableConfig) {
        return new JdbcLookupOptions(
                readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
                readableConfig.get(LOOKUP_CACHE_TTL).toMillis(),
                readableConfig.get(LOOKUP_MAX_RETRIES));
    }

    private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
        final JdbcExecutionOptions.Builder builder = new JdbcExecutionOptions.Builder();
        builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
        return builder.build();
    }

    private JdbcDmlOptions getJdbcDmlOptions(JdbcConnectorOptions jdbcOptions, TableSchema schema) {
        String[] keyFields =
                schema.getPrimaryKey()
                        .map(pk -> pk.getColumns().toArray(new String[0]))
                        .orElse(null);

        return JdbcDmlOptions.builder()
                .withTableName(jdbcOptions.getTableName())
                .withDialect(jdbcOptions.getDialect())
                .withFieldNames(schema.getFieldNames())
                .withKeyFields(keyFields)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DRIVER);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_AUTO_COMMIT);
        optionalOptions.add(SCAN_PARTITION_COLUMN_STRING_TYPE);
        optionalOptions.add(SCAN_PARTITION_STRING_COLUMN_VALUES);
        optionalOptions.add(SCAN_PUSH_DOWN_CONSTRAINTS);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(MAX_RETRY_TIMEOUT);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

        if (config.getOptional(SCAN_PARTITION_COLUMN).isPresent()) {
            checkAllOrNone(
                    config,
                    new ConfigOption[] {
                        SCAN_PARTITION_NUM, SCAN_PARTITION_LOWER_BOUND, SCAN_PARTITION_UPPER_BOUND
                    });
            checkAllOrNone(
                    config,
                    new ConfigOption[] {
                        SCAN_PARTITION_COLUMN_STRING_TYPE, SCAN_PARTITION_STRING_COLUMN_VALUES
                    });
        }

        if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
                && config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
            long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
            long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
            if (lowerBound > upperBound) {
                throw new IllegalArgumentException(
                        String.format(
                                "'%s'='%s' must not be larger than '%s'='%s'.",
                                SCAN_PARTITION_LOWER_BOUND.key(),
                                lowerBound,
                                SCAN_PARTITION_UPPER_BOUND.key(),
                                upperBound));
            }
        }

        checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
        }

        if (config.get(MAX_RETRY_TIMEOUT).getSeconds() <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
                            MAX_RETRY_TIMEOUT.key(),
                            config.get(
                                    ConfigOptions.key(MAX_RETRY_TIMEOUT.key())
                                            .stringType()
                                            .noDefaultValue())));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }
}
