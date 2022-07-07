package com.rovo98.manul.flink.connector.jdbc.utils;

/** Token types define for sql where clause. */
public enum TokenType {
    // Single-character tokens
    LEFT_PAREN, // (
    RIGHT_PAREN, // )
    LEFT_BRACE, // {
    RIGHT_BRACE, // }
    COMMA, // ,
    DOT, // .
    MINUS, // -
    PLUS, // +
    SEMICOLON, // ;
    SLASH, // /
    STAR, // *

    // One or two character tokens
    BANG, // !
    BANG_EQUAL, // !=
    EQUAL, // =
    EQUAL_EQUAL, // ==
    GREATER, // >
    GREATER_EQUAL, // >=
    LESS, // <
    LESS_EQUAL, // <=

    // Literals
    IDENTIFIER,
    STRING,
    NUMBER,

    // Keywords
    AND,
    OR,
    EXISTS,
    IN,
    IS,
    NOT,
    NULL,

    EOF,
}
