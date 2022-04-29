package com.rovo98.flink.manul.connector.jdbc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.rovo98.flink.manul.connector.jdbc.utils.TokenType.*;

public class SimpleSQLWhereClauseScanner {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSQLWhereClauseScanner.class);

    private final String source;
    private final List<Token> tokens = new LinkedList<>();
    private int current = 0;
    private int start = 0;
    private int line = 1;

    private static final Map<String, TokenType> keywords =
            new HashMap<String, TokenType>() {
                {
                    put("and", AND);
                    put("or", OR);
                    put("exists", EXISTS);
                    put("in", IN);
                    put("is", IS);
                    put("not", NOT);
                    put("null", NULL);
                }
            };

    public SimpleSQLWhereClauseScanner(String source) {
        this.source = source;
    }

    public List<Token> scanTokens() {
        while (!isAtEnd()) {
            start = current;
            scanToken();
        }
        tokens.add(new Token(EOF, "", null, line));
        return tokens;
    }

    private boolean isAtEnd() {
        return current >= source.length();
    }

    private void scanToken() {
        char c = advance();
        switch (c) {
            case '(':
                addToken(LEFT_PAREN);
                break;
            case ')':
                addToken(RIGHT_PAREN);
                break;
            case '{':
                addToken(LEFT_BRACE);
                break;
            case '}':
                addToken(RIGHT_BRACE);
                break;
            case ',':
                addToken(COMMA);
                break;
            case '.':
                addToken(DOT);
                break;
            case '+':
                addToken(PLUS);
                break;
            case '-':
                if (match('-')) {
                    // handle sql comment
                    // A comment goes until the end of the line
                    while (peek() != '\n' && !isAtEnd()) advance();
                } else {
                    addToken(MINUS);
                }
                break;
            case ';':
                addToken(SEMICOLON);
                break;
            case '*':
                addToken(STAR);
                break;
            case '!':
                addToken(match('=') ? BANG_EQUAL : BANG);
                break;
            case '=':
                addToken(match('=') ? EQUAL_EQUAL : EQUAL);
                break;
            case '<':
                addToken(match('=') ? LESS_EQUAL : LESS);
                break;
            case '>':
                addToken(match('=') ? GREATER_EQUAL : GREATER);
                break;
            case '/':
                addToken(SLASH);
                break;
            case '\'':
                string();
                break; // handle string literal
            case ' ':
            case '\r':
            case '\t':
                // Ignore meaningless characters (does not needed for parser)
                break;
            case '\n':
                line++;
                break;

            default:
                if (isDigit(c)) {
                    number();
                } else if (isAlpha(c)) {
                    identifier();
                } else {
                    LOG.error("Unexpected character -> {} at line {}.", c, line);
                }
                break;
        }
    }

    private void identifier() {
        while (isAlphaNumeric(peek())) advance();

        String text = source.substring(start, current);
        TokenType type = keywords.get(text.toLowerCase());
        if (type == null) type = IDENTIFIER;
        addToken(type);
    }

    private boolean isAlpha(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private boolean isAlphaNumeric(char c) {
        return isAlpha(c) || isDigit(c);
    }

    private boolean isDigit(char c) {
        return c >= '0' && c < '9';
    }

    private void number() {
        while (isDigit(peek())) advance();

        // Look for a fractional part
        if (peek() == '.' && isDigit(peekNext())) {
            // Consume the '.'
            advance();
            while (isDigit(peek())) advance();
        }

        addToken(NUMBER, Double.parseDouble(source.substring(start, current)));
    }

    private char peekNext() {
        if (current + 1 >= source.length()) return '\0';
        return source.charAt(current + 1);
    }

    private void string() {
        while (peek() != '\'' && !isAtEnd()) {
            if (peek() == '\n') line++;
            advance();
        }
        if (isAtEnd()) {
            LOG.error("Unexpected string at line {}.", line);
            return;
        }

        // The closing '.
        advance();

        // Trim the surrounding quotes.
        String value = source.substring(start + 1, current - 1);
        addToken(STRING, value);
    }

    private char peek() {
        if (isAtEnd()) return '\0';
        return source.charAt(current);
    }

    private boolean match(char expected) {
        if (isAtEnd()) return false;
        if (source.charAt(current) != expected) return false;
        current++;
        return true;
    }

    private void addToken(TokenType tokenType) {
        addToken(tokenType, null);
    }

    private void addToken(TokenType type, Object literal) {
        String text = source.substring(start, current);
        tokens.add(new Token(type, text, literal, line));
    }

    private char advance() {
        return source.charAt(current++);
    }
}
