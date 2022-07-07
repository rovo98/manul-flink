package com.rovo98.manul.flink.connector.jdbc;

import org.apache.flink.util.ExceptionUtils;

import java.util.function.Function;

/**
 * {@link Function} interface which can throw exceptions.
 *
 * @param <T> type of the input type
 * @param <R> type of the return type
 * @param <E> type of the exception which can be thrown
 */
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Throwable> {
    /**
     * Apply the given values t to obtain the resulting value. The operation can throw an exception.
     *
     * @param t type of the input type
     * @return result value
     * @throws E if the operation fails
     */
    R apply(T t) throws E;

    /**
     * Convert a {@link FunctionWithException} into a {@link Function}.
     *
     * @param functionWithException function with exception to convert into a function
     * @param <A> input type
     * @param <B> output type
     * @return {@link Function} which throws all checked exception as an unchecked exception.
     */
    static <A, B> Function<A, B> unchecked(FunctionWithException<A, B, ?> functionWithException) {
        return (A a) -> {
            try {
                return functionWithException.apply(a);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
                // we need this to appease the compiler :-(
                return null;
            }
        };
    }
}
