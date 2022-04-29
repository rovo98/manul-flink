package com.rovo98.flink.manul.connector.jdbc.utils;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class WhereClauseIdentifierConverter {

    private final SimpleSQLWhereClauseScanner tokenizer;
    private final JdbcDialect jdbcDialect;

    public WhereClauseIdentifierConverter(String whereClause, JdbcDialect jdbcDialect) {
        Preconditions.checkNotNull(whereClause);
        Preconditions.checkNotNull(jdbcDialect);
        this.jdbcDialect = jdbcDialect;
        tokenizer = new SimpleSQLWhereClauseScanner(whereClause);
    }

    public String convert() {
        List<Token> tokens = tokenizer.scanTokens();

        StringBuilder sb = new StringBuilder();

        tokens.forEach(
                token -> {
                    if (token.type == TokenType.IDENTIFIER) {
                        sb.append(" ")
                                .append(jdbcDialect.quoteIdentifier(token.lexeme))
                                .append(" ");
                    } else {
                        sb.append(" ").append(token.lexeme).append(" ");
                    }
                });

        return sb.toString().trim();
    }
}
