/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rovo98.flink.manul.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class for loading JDBC dialects dynamically. */
public class JdbcDialectService {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectService.class);

    private JdbcDialectService() {}

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database url
     * @throws IllegalStateException if this service cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     * @return The loaded dialect
     */
    public static JdbcDialect load(String url) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialect> foundDialects = discoverJdbcDialects(cl);

        if (foundDialects.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc dialects that implement '%s' in the classpath",
                            JdbcDialect.class.getName()));
        }

        final List<JdbcDialect> matchingDialects =
                foundDialects.stream().filter(d -> d.canHandle(url)).collect(Collectors.toList());

        if (matchingDialects.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any jdbc dialects that can handle url '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available dialects are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialect.class.getName(),
                            foundDialects.stream()
                                    .map(d -> d.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        if (matchingDialects.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple jdbc dialects can handle url '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous dialect classes are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialect.class,
                            matchingDialects.stream()
                                    .map(d -> d.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingDialects.get(0);
    }

    private static List<JdbcDialect> discoverJdbcDialects(ClassLoader classLoader) {
        try {
            final Map<String, JdbcDialect> result = new HashMap<>();
            ServiceLoader.load(JdbcDialect.class, classLoader)
                    .iterator()
                    .forEachRemaining(
                            jdbcDialect -> {
                                String key = jdbcDialect.getClass().getSimpleName();
                                result.put(key, jdbcDialect);
                            });
            return new LinkedList<>(result.values());
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for the jdbc dialect.", e);
            throw new RuntimeException("Could not load service provider for jdbc dialect.", e);
        }
    }
}
