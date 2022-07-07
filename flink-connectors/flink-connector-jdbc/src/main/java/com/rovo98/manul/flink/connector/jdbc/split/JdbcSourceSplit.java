package com.rovo98.manul.flink.connector.jdbc.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/** JdbcSourceSplit. */
public class JdbcSourceSplit implements SourceSplit, Serializable {
    public static final long serialVersionUID = 1L;

    private final String sql;
    private final String splitId;

    public JdbcSourceSplit(String sql) {
        this(sql, UUID.randomUUID().toString());
    }

    public JdbcSourceSplit(String sql, String splitId) {
        this.sql = sql;
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    public String getQuery() {
        return sql;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JdbcSourceSplit that = (JdbcSourceSplit) o;
        return Objects.equals(sql, that.sql) && Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, splitId);
    }
}
