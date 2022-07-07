package com.rovo98.manul.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/** OracleRowConverter. */
public class OracleRowConverter extends AbstractJdbcRowConverter {

    public OracleRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "Oracle";
    }
}
