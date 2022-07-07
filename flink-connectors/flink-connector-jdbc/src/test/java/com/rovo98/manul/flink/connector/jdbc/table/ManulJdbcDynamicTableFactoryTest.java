package com.rovo98.manul.flink.connector.jdbc.table;

import com.rovo98.manul.flink.connector.jdbc.dialect.JdbcDialectService;
import com.rovo98.manul.flink.connector.jdbc.internal.options.ManulJdbcReadOptions;

import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.rovo98.manul.flink.connector.jdbc.factories.utils.FactoryMocks.createTableSource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

/** Test for {@link ManulJdbcDynamicTableFactory} Adopted from flink. */
public class ManulJdbcDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("bbb", "aaa")));

    @Test
    public void testJdbcCommonProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        properties.put("username", "user");
        properties.put("password", "pass");
        properties.put("connection.max-retry-timeout", "120s");

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        JdbcConnectorOptions options =
                JdbcConnectorOptions.builder()
                        .setDBUrl("jdbc:derby:memory:mydb")
                        .setTableName("mytable")
                        .setDialect(JdbcDialectService.load("jdbc:derby:memory:mydb"))
                        .setUsername("user")
                        .setPassword("pass")
                        .setConnectionCheckTimeoutSeconds(120)
                        .build();
        JdbcLookupOptions lookupOptions =
                JdbcLookupOptions.builder()
                        .setCacheMaxSize(-1)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(3)
                        .build();
        ManulJdbcDynamicTableSource expectedSource =
                new ManulJdbcDynamicTableSource(
                        options,
                        ManulJdbcReadOptions.builder().build(),
                        lookupOptions,
                        TableSchema.fromResolvedSchema(SCHEMA));
        assertEquals(expectedSource, actualSource);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "manul-jdbc");
        options.put("url", "jdbc:derby:memory:mydb");
        options.put("table-name", "mytable");
        return options;
    }
}
