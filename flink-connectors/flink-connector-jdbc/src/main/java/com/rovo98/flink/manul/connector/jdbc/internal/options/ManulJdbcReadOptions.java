package com.rovo98.flink.manul.connector.jdbc.internal.options;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** Options for the JDBC scan. */
public class ManulJdbcReadOptions implements Serializable {

    private final String query;
    private final String partitionColumnName;
    private final Long partitionLowerBound;
    private final Long partitionUpperBound;
    private final Integer numPartitions;

    private final String stringPartitionColumnValues;
    private final boolean partitionColumnStringType;
    private final String whereClausePushDownConstraints;

    private final int fetchSize;
    private final boolean autoCommit;

    private ManulJdbcReadOptions(
            String query,
            String partitionColumnName,
            Long partitionLowerBound,
            Long partitionUpperBound,
            Integer numPartitions,
            String stringPartitionColumnValues,
            boolean partitionColumnStringType,
            String whereClausePushDownConstraints,
            int fetchSize,
            boolean autoCommit) {
        this.query = query;
        this.partitionColumnName = partitionColumnName;
        this.partitionLowerBound = partitionLowerBound;
        this.partitionUpperBound = partitionUpperBound;
        this.numPartitions = numPartitions;

        this.stringPartitionColumnValues = stringPartitionColumnValues;
        this.partitionColumnStringType = partitionColumnStringType;
        this.whereClausePushDownConstraints = whereClausePushDownConstraints;

        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
    }

    public Optional<String> getQuery() {
        return Optional.ofNullable(query);
    }

    public Optional<String> getPartitionColumnName() {
        return Optional.ofNullable(partitionColumnName);
    }

    public Optional<Long> getPartitionLowerBound() {
        return Optional.ofNullable(partitionLowerBound);
    }

    public Optional<Long> getPartitionUpperBound() {
        return Optional.ofNullable(partitionUpperBound);
    }

    public Optional<Integer> getNumPartitions() {
        return Optional.ofNullable(numPartitions);
    }

    public Optional<String> getStringPartitionColumnValues() {
        return Optional.ofNullable(stringPartitionColumnValues);
    }

    public boolean getPartitionColumnStringType() {
        return partitionColumnStringType;
    }

    public Optional<String> getWhereClausePushDownConstraints() {
        return Optional.ofNullable(whereClausePushDownConstraints);
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public static ManulJdbcReadOptions.Builder builder() {
        return new ManulJdbcReadOptions.Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ManulJdbcReadOptions) {
            ManulJdbcReadOptions options = (ManulJdbcReadOptions) o;
            return Objects.equals(query, options.query)
                    && Objects.equals(partitionColumnName, options.partitionColumnName)
                    && Objects.equals(partitionLowerBound, options.partitionLowerBound)
                    && Objects.equals(partitionUpperBound, options.partitionUpperBound)
                    && Objects.equals(numPartitions, options.numPartitions)
                    && Objects.equals(stringPartitionColumnValues, options.stringPartitionColumnValues)
                    && Objects.equals(partitionColumnStringType, options.partitionColumnStringType)
                    && Objects.equals(whereClausePushDownConstraints, options.whereClausePushDownConstraints)
                    && Objects.equals(fetchSize, options.fetchSize)
                    && Objects.equals(autoCommit, options.autoCommit);
        } else {
            return false;
        }
    }

    /** Builder of {@link com.rovo98.flink.manul.connector.jdbc.internal.options.ManulJdbcReadOptions}. */
    public static class Builder {
        protected String query;
        protected String partitionColumnName;
        protected Long partitionLowerBound;
        protected Long partitionUpperBound;
        protected Integer numPartitions;

        protected String stringPartitionColumnValues;
        protected boolean partitionColumnStringType = false;
        protected String whereClausePushDownConstraints;

        protected int fetchSize = 0;
        protected boolean autoCommit = true;

        /** optional, SQL query statement for this JDBC source. */
        public ManulJdbcReadOptions.Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        /** optional, name of the column used for partitioning the input. */
        public ManulJdbcReadOptions.Builder setPartitionColumnName(String partitionColumnName) {
            this.partitionColumnName = partitionColumnName;
            return this;
        }

        /** optional, the smallest value of the first partition. */
        public ManulJdbcReadOptions.Builder setPartitionLowerBound(long partitionLowerBound) {
            this.partitionLowerBound = partitionLowerBound;
            return this;
        }

        /** optional, the largest value of the last partition. */
        public ManulJdbcReadOptions.Builder setPartitionUpperBound(long partitionUpperBound) {
            this.partitionUpperBound = partitionUpperBound;
            return this;
        }

        /**
         * optional, the maximum number of partitions that can be used for parallelism in table
         * reading.
         */
        public ManulJdbcReadOptions.Builder setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public ManulJdbcReadOptions.Builder setStringPartitionColumnValues(String stringPartitionColumnValues) {
            this.stringPartitionColumnValues = stringPartitionColumnValues;
            return this;
        }

        public ManulJdbcReadOptions.Builder setPartitionColumnStringType(boolean partitionColumnStringType) {
            this.partitionColumnStringType = partitionColumnStringType;
            return this;
        }

        public ManulJdbcReadOptions.Builder setWhereClausePushDownConstraints(String whereClausePushDownConstraints) {
            this.whereClausePushDownConstraints = whereClausePushDownConstraints;
            return this;
        }

        /**
         * optional, the number of rows to fetch per round trip. default value is 0, according to
         * the jdbc api, 0 means that fetchSize hint will be ignored.
         */
        public ManulJdbcReadOptions.Builder setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        /** optional, whether to set auto commit on the JDBC driver. */
        public ManulJdbcReadOptions.Builder setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public ManulJdbcReadOptions build() {
            return new ManulJdbcReadOptions(
                    query,
                    partitionColumnName,
                    partitionLowerBound,
                    partitionUpperBound,
                    numPartitions,
                    stringPartitionColumnValues,
                    partitionColumnStringType,
                    whereClausePushDownConstraints,
                    fetchSize,
                    autoCommit);
        }
    }
}
