package com.rovo98.flink.manul.connector.jdbc;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ManulJdbcInputFormat<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(ManulJdbcInputFormat.class);

    private final JdbcConnectionProvider connectionProvider;
    private final int fetchSize;
    private final Boolean autoCommit;
    private Object[] parameterValues;
    private final String queryTemplate;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final ResultSetValueExtractor<T> resultSetValueExtractor;

    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private transient boolean hasNext;

    private ManulJdbcInputFormat(
            JdbcConnectionProvider connectionProvider,
            int fetchSize,
            Boolean autoCommit,
            Object[] parameterValues,
            String queryTemplate,
            int resultSetType,
            int resultSetConcurrency,
            ResultSetValueExtractor<T> resultSetValueExtractor) {
        this.connectionProvider = connectionProvider;
        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
        this.parameterValues = parameterValues;
        this.queryTemplate = queryTemplate;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetValueExtractor = resultSetValueExtractor;
    }

    protected void setQueryParameterValues(Object[] parameterValues) {
        this.parameterValues = parameterValues;
    }

    public void openInputFormat() {
        // called one per inputFormat
        try {
            Connection dbConn = connectionProvider.getOrEstablishConnection();
            // set autoCommit mode only if it was explicitly configured.
            // keep connection default otherwise.
            if (autoCommit != null) {
                dbConn.setAutoCommit(autoCommit);
            }
            statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
            if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                    "JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
        }

    }

    public void setParameterValueAndExecute() {
        try {
            if (parameterValues != null) {
                for (int i = 0; i < parameterValues.length; i++) {
                    Object param = parameterValues[i];
                    if (param instanceof String) {
                        statement.setString(i + 1, (String) param);
                    } else if (param instanceof Long) {
                        statement.setLong(i + 1, (Long) param);
                    } else if (param instanceof Integer) {
                        statement.setInt(i + 1, (Integer) param);
                    } else if (param instanceof Double) {
                        statement.setDouble(i + 1, (Double) param);
                    } else if (param instanceof Boolean) {
                        statement.setBoolean(i + 1, (Boolean) param);
                    } else if (param instanceof Float) {
                        statement.setFloat(i + 1, (Float) param);
                    } else if (param instanceof BigDecimal) {
                        statement.setBigDecimal(i + 1, (BigDecimal) param);
                    } else if (param instanceof Byte) {
                        statement.setByte(i + 1, (Byte) param);
                    } else if (param instanceof Short) {
                        statement.setShort(i + 1, (Short) param);
                    } else if (param instanceof Date) {
                        statement.setDate(i + 1, (Date) param);
                    } else if (param instanceof Time) {
                        statement.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        statement.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof Array) {
                        statement.setArray(i + 1, (Array) param);
                    } else {
                        // extends with other types if needed
                        throw new IllegalArgumentException(
                                "setParameterValuesAndExecute() failed. Parameter "
                                        + i
                                        + " of type "
                                        + param.getClass()
                                        + " is not handled (yet).");
                    }
                }
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("setParameterValuesAndExecute()() failed." + se.getMessage(), se);
        }
    }

    /**
     * Returns an iterator of fetched jdbc records.
     *
     * @return an iterator of fetched jdbc records; {@code null} if no record fetched.
     */
    public Iterator<T> readSplit() {
        List<T> records = new LinkedList<>();
        this.setParameterValueAndExecute();
        if (!hasNext) {
            return null;
        }
        try {
            while (hasNext) {
                records.add(resultSetValueExtractor.apply(resultSet));
                hasNext = resultSet.next();
            }
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException("readSplit from jdbc failed - " + se.getMessage(), se);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException se) {
                    LOG.info("InputFormat ResultSet couldn't be closed - " + se.getMessage());
                }
            }
        }

        return records.iterator();
    }

    public void closeInputFormat() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException se) {
                LOG.info("InputFormat statement couldn't be closed - " + se.getMessage());
            } finally {
                statement = null;
            }
        }
        connectionProvider.closeConnection();
        parameterValues = null;
    }

    /**
     * A builder used to parameters to the input format's configuration in a fluent way.
     *
     * @return builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private final JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
        private int fetchSize;
        private Boolean autoCommit;
        private Object[] parameterValues;
        private String queryTemplate;
        private ResultSetValueExtractor<T> resultSetValueExtractor;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

        public Builder() {
            this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        }

        public Builder<T> setDriverName(String driverName) {
            this.connOptionsBuilder.withDriverName(driverName);
            return this;
        }

        public Builder<T> setDBUrl(String dbURL) {
            this.connOptionsBuilder.withUrl(dbURL);
            return this;
        }

        public Builder<T> setUsername(String username) {
            this.connOptionsBuilder.withUsername(username);
            return this;
        }

        public Builder<T> setPassword(String password) {
            this.connOptionsBuilder.withPassword(password);
            return this;
        }

        public Builder<T> setQuery(String query) {
            this.queryTemplate = query;
            return this;
        }

        public Builder<T> setParameterValues(Object[] parameterValues) {
            this.parameterValues = parameterValues;
            return this;
        }

        public Builder<T> setResultSetValueExtractor(
                ResultSetValueExtractor<T> resultSetValueExtractor) {
            this.resultSetValueExtractor = resultSetValueExtractor;
            return this;
        }

        /** By default, fetchSize is set to 0 which means this hints will be ignored. */
        public Builder<T> setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder<T> setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder<T> setResultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        public Builder<T> setResultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public ManulJdbcInputFormat<T> build() {
            if (this.queryTemplate == null) {
                throw new NullPointerException("No query supplied");
            }
            if (this.resultSetValueExtractor == null) {
                throw new NullPointerException("No resultSet value extractor supplied");
            }
            return new ManulJdbcInputFormat<>(
                    new SimpleJdbcConnectionProvider(connOptionsBuilder.build()),
                    this.fetchSize,
                    this.autoCommit,
                    this.parameterValues,
                    this.queryTemplate,
                    this.resultSetType,
                    this.resultSetConcurrency,
                    this.resultSetValueExtractor);
        }
    }
}
