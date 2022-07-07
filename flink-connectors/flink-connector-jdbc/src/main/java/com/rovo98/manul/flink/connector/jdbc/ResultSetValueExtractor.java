package com.rovo98.manul.flink.connector.jdbc;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Extract field values from the given ResultSet and convert to target type T.
 *
 * @param <T> type of the extracted data record
 */
@FunctionalInterface
public interface ResultSetValueExtractor<T>
        extends FunctionWithException<ResultSet, T, SQLException>, Serializable {}
