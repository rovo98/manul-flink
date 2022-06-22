package com.rovo98.flink.manul.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/** JdbcDialectServiceTest. */
public class JdbcDialectServiceTest {

    @Test
    public void testLoadingJdbcDialects() {
        JdbcDialect mySQLDialect =
                JdbcDialectService.load("jdbc:mysql://example_host:3306/example_db");
        assertEquals("MySQL", mySQLDialect.dialectName());

        JdbcDialect derbyDialect =
                JdbcDialectService.load("jdbc:derby://example_host:1527/example_db");
        assertEquals("Derby", derbyDialect.dialectName());

        JdbcDialect postgresDialect =
                JdbcDialectService.load("jdbc:postgresql://example_host:5432/example_db");
        assertEquals("PostgreSQL", postgresDialect.dialectName());

        JdbcDialect oracleDialect = JdbcDialectService.load("jdbc:oracle://example_host:3306/test");
        assertEquals("Oracle", oracleDialect.dialectName());
    }

    @Test
    @DisplayName("Test loading not existed Jdbc Dialect")
    public void testLoadNotExistedJdbcDialect() {
        String url = "jdbc:noexisted://example_host:9870/example_db";
        Exception exception =
                assertThrows(IllegalStateException.class, () -> JdbcDialectService.load(url));

        Class<?>[] builtinDialects =
                new Class[] {
                    DerbyDialect.class,
                    MySQLDialect.class,
                    PostgresDialect.class,
                    OracleDialect.class
                };

        String expectedExceptionMsg =
                String.format(
                        "Could not find any jdbc dialects that can handle url '%s' that implements '%s' in the classpath.\n\n"
                                + "Available dialects are:\n\n"
                                + "%s",
                        url,
                        JdbcDialect.class.getName(),
                        Arrays.stream(builtinDialects)
                                .map(Class::getName)
                                .distinct()
                                .sorted()
                                .collect(Collectors.joining("\n")));

        assertEquals(expectedExceptionMsg, exception.getMessage());
    }
}
