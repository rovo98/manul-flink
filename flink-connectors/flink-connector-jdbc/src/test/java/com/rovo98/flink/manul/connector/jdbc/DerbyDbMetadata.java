package com.rovo98.flink.manul.connector.jdbc;

import javax.sql.XADataSource;

import org.apache.derby.jdbc.EmbeddedXADataSource;

/** DerbyDbMetadata. */
public class DerbyDbMetadata implements DbMetadata {
    private final String dbName;
    private final String dbInitUrl;
    private final String url;

    public DerbyDbMetadata(String schemaName) {
        dbName = "memory:" + schemaName;
        url = "jdbc:derby:" + dbName;
        dbInitUrl = url + ";create=true";
    }

    @Override
    public String getInitUrl() {
        return dbInitUrl;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public XADataSource buildXaDataSource() {
        EmbeddedXADataSource ds = new EmbeddedXADataSource();
        ds.setDatabaseName(dbName);
        return ds;
    }

    @Override
    public String getDriverClass() {
        return "org.apache.derby.jdbc.EmbeddedDriver";
    }
}
