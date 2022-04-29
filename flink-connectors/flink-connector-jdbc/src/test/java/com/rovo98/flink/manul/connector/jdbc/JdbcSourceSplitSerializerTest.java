package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;
import org.junit.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcSourceSplitSerializerTest {

    @Test
    public void testSerializeAndDeserializeJdbcSourceSplit() throws IOException {
        JdbcSourceSplit split =
                new JdbcSourceSplit("select * from example_tbl where start_date >= ?");

        JdbcSourceSplitSerializer jdbcSourceSplitSerializer = new JdbcSourceSplitSerializer();
        byte[] serialized = jdbcSourceSplitSerializer.serialize(split);
        JdbcSourceSplit deserializedSplit = jdbcSourceSplitSerializer.deserialize(1, serialized);

        assertEquals(split, deserializedSplit);
    }
}
