package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class JdbcSourceSplitSerializer implements SimpleVersionedSerializer<JdbcSourceSplit> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JdbcSourceSplit jdbcSourceSplit) throws IOException {
        Preconditions.checkArgument(
                jdbcSourceSplit.getClass() == JdbcSourceSplit.class,
                "cannot serialize subclasses.");
        final DataOutputSerializer out =
                new DataOutputSerializer(jdbcSourceSplit.getQuery().length() + 36);
        out.writeUTF(jdbcSourceSplit.getQuery());
        out.writeUTF(jdbcSourceSplit.splitId());
        return out.getCopyOfBuffer();
    }

    @Override
    public JdbcSourceSplit deserialize(int version, byte[] bytes) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(bytes);
        return new JdbcSourceSplit(in.readUTF(), in.readUTF());
    }
}
