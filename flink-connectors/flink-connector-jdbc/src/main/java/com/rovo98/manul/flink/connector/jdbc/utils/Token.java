package com.rovo98.manul.flink.connector.jdbc.utils;

/** Token entity which contains all token info. */
public class Token {
    final TokenType type;
    final String lexeme;
    final Object literal;
    final int line;

    public Token(TokenType type, String lexeme, Object literal, int line) {
        this.type = type;
        this.lexeme = lexeme;
        this.literal = literal;
        this.line = line;
    }

    public String toString() {
        return String.format(
                "type: %s, lexeme: %s, literal: %s  at line[%s].", type, lexeme, literal, line);
    }
}
