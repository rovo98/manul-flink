package com.rovo98.flink.manul.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class OracleRowConverter extends AbstractJdbcRowConverter {

    public OracleRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Oracle";
    }
}
