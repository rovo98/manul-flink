package com.rovo98.flink.manul.connector.jdbc.factories.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.Map;

/**
 * Utilities for testing instances usually created by {@link
 * org.apache.flink.table.factories.FactoryUtil}
 */
public class FactoryMocks {

    public static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.STRING()),
                    Column.physical("b", DataTypes.INT()),
                    Column.physical("c", DataTypes.BOOLEAN()));

    public static final DataType PHYSICAL_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    public static final RowType PHYSICAL_TYPE = (RowType) PHYSICAL_DATA_TYPE.getLogicalType();

    public static final ObjectIdentifier IDENTIFIER =
            ObjectIdentifier.of("default", "default", "t1");

    public static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options
    ) {
        return FactoryUtil.createTableSource(
                null,
                IDENTIFIER,
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                Collections.emptyList(),
                                options
                        ),
                        schema
                ),
                new Configuration(),
                FactoryMocks.class.getClassLoader(),
                false);
    }
}
