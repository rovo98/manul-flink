package com.rovo98.flink.manul.connector.jdbc.internal.options;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import javax.annotation.Nullable;

/** Modification of {@link JdbcConnectionOptions}. */
public class ManulJdbcConnectionOptions extends JdbcConnectionOptions {
    private static final long serialVersionUID = 1L;

    private final int fetchSize;
    private final Boolean autoCommit;

    protected ManulJdbcConnectionOptions(
            String url,
            @Nullable String driverName,
            @Nullable String username,
            @Nullable String password,
            int connectionCheckTimeoutSeconds,
            int fetchSize,
            boolean autoCommit) {
        super(url, driverName, username, password, connectionCheckTimeoutSeconds);
        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    /** Returns a builder for ManulJdbcConnectionOptions. */
    public static ManulJdbcConnectionOptionsBuilder builder() {
        return new ManulJdbcConnectionOptionsBuilder();
    }

    /** Builder for {@link ManulJdbcConnectionOptions}. */
    public static class ManulJdbcConnectionOptionsBuilder {
        private String url;
        private String driverName;
        private String username;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;
        private int fetchSize = 0;
        private Boolean autoCommit = true;

        public ManulJdbcConnectionOptionsBuilder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ManulJdbcConnectionOptionsBuilder withDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        public ManulJdbcConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public ManulJdbcConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set the maximum timeout between retries, default is 60 seconds.
         *
         * @param connectionCheckTimeoutSeconds the timeout seconds, shouldn't smaller than 1
         *     second.
         */
        public ManulJdbcConnectionOptionsBuilder withConnectionCheckTimeoutSeconds(
                int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        public ManulJdbcConnectionOptionsBuilder withFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public ManulJdbcConnectionOptionsBuilder withAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public ManulJdbcConnectionOptions build() {
            return new ManulJdbcConnectionOptions(
                    url,
                    driverName,
                    username,
                    password,
                    connectionCheckTimeoutSeconds,
                    fetchSize,
                    autoCommit);
        }
    }
}
