package com.rovo98.manul.flink.connector.jdbc.split;

import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This query parameters generator is a helper class to parameterize from/to queries on a string
 * column.
 *
 * <p>For example, if there's a table <CODE>BOOKS</CODE> with a string column <CODE>name</CODE>,
 * using a query like:
 *
 * <PRE>
 * SELECT * FROM BOOKS WHERE name = ?
 * </PRE>
 */
public class JdbcStringParameterValuesProvider implements JdbcParameterValuesProvider {

    // comma separated values string
    private final List<String> columnValues;

    public JdbcStringParameterValuesProvider(String columnValuesString) {
        Preconditions.checkNotNull(
                columnValuesString, "A comma separated values string must be given!");
        columnValues = extractValuesFromCommaSeparatedString(columnValuesString);
        Preconditions.checkArgument(
                !columnValues.isEmpty(),
                "Extracted column values list is empty! "
                        + "given value string: "
                        + columnValuesString);
    }

    private List<String> extractValuesFromCommaSeparatedString(String s) {
        return Arrays.stream(s.split(",")).map(String::trim).collect(Collectors.toList());
    }

    @Override
    public Serializable[][] getParameterValues() {
        int batchNum = columnValues.size();

        Serializable[][] parameters = new Serializable[batchNum][1];
        for (int i = 0; i < columnValues.size(); i++) {
            parameters[i] = new String[] {columnValues.get(i)};
        }
        return parameters;
    }
}
