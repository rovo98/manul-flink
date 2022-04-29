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

package com.rovo98.flink.manul.connector.jdbc.table;

import com.rovo98.flink.manul.connector.jdbc.internal.options.ManulJdbcReadOptions;
import com.rovo98.flink.manul.connector.jdbc.split.JdbcStringParameterValuesProvider;
import com.rovo98.flink.manul.connector.jdbc.utils.WhereClauseIdentifierConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.connector.jdbc.table.JdbcRowDataLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;

/** A {@link DynamicTableSource} for JDBC. */
public class ManulJdbcDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private final JdbcConnectorOptions options;
    private final ManulJdbcReadOptions readOptions;
    private final JdbcLookupOptions lookupOptions;
    private TableSchema physicalSchema;
    private final String dialectName;
    private long limit = -1;

    public ManulJdbcDynamicTableSource(
            JdbcConnectorOptions options,
            ManulJdbcReadOptions readOptions,
            JdbcLookupOptions lookupOptions,
            TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
        this.physicalSchema = physicalSchema;
        this.dialectName = options.getDialect().dialectName();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // JDBC only support non-nested look up keys
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        return TableFunctionProvider.of(
                new JdbcRowDataLookupFunction(
                        options,
                        lookupOptions,
                        physicalSchema.getFieldNames(),
                        physicalSchema.getFieldDataTypes(),
                        keyNames,
                        rowType));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final JdbcRowDataInputFormat.Builder builder =
                JdbcRowDataInputFormat.builder()
                        .setDrivername(options.getDriverName())
                        .setDBUrl(options.getDbURL())
                        .setUsername(options.getUsername().orElse(null))
                        .setPassword(options.getPassword().orElse(null))
                        .setAutoCommit(readOptions.getAutoCommit());

        if (readOptions.getFetchSize() != 0) {
            builder.setFetchSize(readOptions.getFetchSize());
        }
        final JdbcDialect dialect = options.getDialect();
        String query =
                dialect.getSelectFromStatement(
                        options.getTableName(), physicalSchema.getFieldNames(), new String[0]);
        Optional<String> whereClausePushDownConstraints = Optional.empty();
        if (readOptions.getWhereClausePushDownConstraints().isPresent()) {
            whereClausePushDownConstraints =
                    Optional.of(
                            new WhereClauseIdentifierConverter(
                                            readOptions.getWhereClausePushDownConstraints().get(),
                                            dialect)
                                    .convert());
        }
        if (readOptions.getPartitionColumnName().isPresent()) {
            if (readOptions.getPartitionColumnStringType()) {
                String partitionColumnValues = readOptions.getStringPartitionColumnValues().get();
                builder.setParametersProvider(
                        new JdbcStringParameterValuesProvider(partitionColumnValues));
                query +=
                        " WHERE "
                                + dialect.quoteIdentifier(
                                        readOptions.getPartitionColumnName().get())
                                + " = ?";
            } else {
                long lowerBound = readOptions.getPartitionLowerBound().get();
                long upperBound = readOptions.getPartitionUpperBound().get();
                int numPartitions = readOptions.getNumPartitions().get();
                builder.setParametersProvider(
                        new JdbcNumericBetweenParametersProvider(lowerBound, upperBound)
                                .ofBatchNum(numPartitions));
                query +=
                        " WHERE "
                                + dialect.quoteIdentifier(
                                        readOptions.getPartitionColumnName().get())
                                + " BETWEEN ? AND ?";
            }
            if (whereClausePushDownConstraints.isPresent()) {
                query += " AND " + whereClausePushDownConstraints.get();
            }
        } else {
            if (whereClausePushDownConstraints.isPresent()) {
                query += " WHERE " + whereClausePushDownConstraints.get();
            }
        }
        if (limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }
        builder.setQuery(query);
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        builder.setRowConverter(dialect.getRowConverter(rowType));
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public DynamicTableSource copy() {
        return new ManulJdbcDynamicTableSource(options, readOptions, lookupOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ManulJdbcDynamicTableSource)) {
            return false;
        }
        ManulJdbcDynamicTableSource that = (ManulJdbcDynamicTableSource) o;
        return Objects.equals(options, that.options)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(dialectName, that.dialectName)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                options, readOptions, lookupOptions, physicalSchema, dialectName, limit);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }
}
