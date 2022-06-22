package com.rovo98.flink.manul.connector.jdbc.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Test;

/** SimpleSQLWhereClauseScannerTest. */
public class SimpleSQLWhereClauseScannerTest {

    @Test
    public void testTokenizeValidSqlWhereClause() {
        String validWhereClause =
                "-- \n p_id is not null and id = 100 and start_date >= '2022-04-11' \n"
                        + "and code in ('1', '2', '3')";
        SimpleSQLWhereClauseScanner scanner = new SimpleSQLWhereClauseScanner(validWhereClause);
        List<Token> tokens = scanner.scanTokens();
        Set<Object> identifierSet =
                tokens.stream()
                        .filter(t -> t.type == TokenType.IDENTIFIER)
                        .map(t -> t.lexeme)
                        .collect(Collectors.toSet());

        assertAll(
                "filtered identifiers",
                () -> assertEquals(identifierSet.size(), 4),
                () -> assertTrue(identifierSet.contains("p_id")),
                () -> assertTrue(identifierSet.contains("id")),
                () -> assertTrue(identifierSet.contains("start_date")),
                () -> assertTrue(identifierSet.contains("code")));
    }
}
