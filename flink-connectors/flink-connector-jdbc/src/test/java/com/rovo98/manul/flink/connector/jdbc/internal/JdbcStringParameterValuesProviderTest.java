package com.rovo98.manul.flink.connector.jdbc.internal;

import com.rovo98.manul.flink.connector.jdbc.split.JdbcStringParameterValuesProvider;

import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

/** JdbcStringParameterValuesProviderTest. */
public class JdbcStringParameterValuesProviderTest {

    @Test
    public void testGetParameterValuesFromValidCommaString() {
        StringBuilder sb = new StringBuilder("1");
        for (int i = 2; i < 6; i++) {
            sb.append(", ").append(i);
        }
        String paramValueString = sb.toString();
        JdbcParameterValuesProvider jdbcParameterValuesProvider =
                new JdbcStringParameterValuesProvider(paramValueString);

        Serializable[][] actual = jdbcParameterValuesProvider.getParameterValues();
        Serializable[][] expected = new Serializable[5][1];
        for (int i = 0; i < 5; i++) {
            expected[i] = new String[] {"" + (i + 1)};
        }
        assertAll(
                "compareSerializableParameters",
                () -> {
                    for (int i = 0; i < expected.length; i++) {
                        assertEquals(expected[i][0], actual[i][0]);
                    }
                });
    }
}
