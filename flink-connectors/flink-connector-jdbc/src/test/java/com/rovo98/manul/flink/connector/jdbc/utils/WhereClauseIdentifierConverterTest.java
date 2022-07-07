package com.rovo98.manul.flink.connector.jdbc.utils;

import com.rovo98.manul.flink.connector.jdbc.dialect.MySQLDialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

/** WhereClauseIdentifierConverterTest. */
public class WhereClauseIdentifierConverterTest {

    @Test
    public void testConvertGivenSQLWhereClauseIdentifiers() {
        String validWhereClause =
                "-- \n p_id is not null and id = 100 and start_date >= '2022-04-11' \n"
                        + "and code in ('1', '2', '3')";
        JdbcDialect mySQLDialect = new MySQLDialect();
        WhereClauseIdentifierConverter converter =
                new WhereClauseIdentifierConverter(validWhereClause, mySQLDialect);

        String actual = converter.convert();
        String expected =
                "`p_id`  is  not  null  and  `id`  =  100  and"
                        + "  `start_date`  >=  '2022-04-11'  and  `code`  in  (  '1'  ,  '2'  ,  '3'  )";
        assertEquals(expected, actual);
    }
}
