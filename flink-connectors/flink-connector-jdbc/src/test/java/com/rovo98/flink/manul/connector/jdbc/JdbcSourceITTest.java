package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.internal.options.ManulJdbcConnectionOptions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.rovo98.flink.manul.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static com.rovo98.flink.manul.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static com.rovo98.flink.manul.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static com.rovo98.flink.manul.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static com.rovo98.flink.manul.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Before;
import org.junit.Test;

/** JdbcSourceITTest. */
public class JdbcSourceITTest extends JdbcTestBase {

    @Before
    public void before() throws Exception {
        super.before();
        // preparation
        int[] insertRet = insertBooks();
        long insertedRows = Arrays.stream(insertRet).filter(i -> i >= 0).count();

        assertEquals(TEST_DATA.length, insertedRows);
    }

    @Test
    public void testJdbcSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);

        JdbcSource<TestEntry> source =
                JdbcSource.source(
                        "select * from " + INPUT_TABLE + " where price is not null",
                        null, // no parameters need to provided
                        (rs ->
                                new TestEntry(
                                        rs.getInt("id"),
                                        rs.getString("title"),
                                        rs.getString("author"),
                                        rs.getDouble("price"),
                                        rs.getInt("qty"))),
                        ManulJdbcConnectionOptions.builder()
                                .withDriverName(getDbMetadata().getDriverClass())
                                .withUrl(getDbMetadata().getUrl())
                                .build());

        DataStream<TestEntry> entryStream =
                env.fromSource(
                        source,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "fetchInsertedBooks",
                        TypeInformation.of(TestEntry.class));

        List<TestEntry> results = new ArrayList<>();
        try (CloseableIterator<TestEntry> itr = entryStream.executeAndCollect()) {
            itr.forEachRemaining(results::add);
        }

        List<TestEntry> expected =
                Arrays.stream(TEST_DATA).filter(r -> r.price != null).collect(Collectors.toList());
        assertEquals(expected, results);
    }

    private int[] insertBooks() throws SQLException {
        try (Connection connection = DriverManager.getConnection(getDbMetadata().getUrl())) {
            try (PreparedStatement st =
                    connection.prepareStatement(String.format(INSERT_TEMPLATE, INPUT_TABLE))) {
                for (TestEntry entry : TEST_DATA) {
                    st.setInt(1, entry.id);
                    st.setString(2, entry.title);
                    st.setString(3, entry.author);
                    if (entry.price == null) {
                        st.setNull(4, Types.DOUBLE);
                    } else {
                        st.setDouble(4, entry.price);
                    }
                    st.setInt(5, entry.qty);
                    st.addBatch();
                }
                return st.executeBatch();
            }
        }
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }
}
