package com.rovo98.flink.manul.connector.jdbc;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.sql.XADataSource;
import java.io.Serializable;

/** Describes a database: driver, schema and urls. */
public interface DbMetadata extends Serializable {

    String getInitUrl();

    String getUrl();

    default String getUser() {
        return "";
    }

    default String getPassword() {
        return "";
    }

    XADataSource buildXaDataSource();

    String getDriverClass();

    default JdbcConnectionOptions toConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(getDriverClass())
                .withUrl(getUrl())
                .build();
    }
}
