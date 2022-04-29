package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcEnumeratorCheckpointSerializerTest {

    @Test
    public void testSerializeAndDeserializeJdbcEnumeratorCheckpoint() throws IOException {
        final String query = "select * from ex.fake_users where username like ?";
        Map<String, JdbcSourceSplit> splitMap = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            JdbcSourceSplit split = new JdbcSourceSplit(query);
            splitMap.put(split.splitId(), split);
        }

        JdbcEnumeratorCheckpointSerializer serializer = new JdbcEnumeratorCheckpointSerializer();
        byte[] serialized = serializer.serialize(splitMap.values());

        Collection<JdbcSourceSplit> deserializedSplits = serializer.deserialize(1, serialized);
        deserializedSplits.forEach(split -> assertEquals(splitMap.get(split.splitId()), split));
    }
}
