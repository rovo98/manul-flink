package com.rovo98.manul.flink.connector.jdbc;

import com.rovo98.manul.flink.connector.jdbc.split.JdbcSourceSplit;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

/** JdbcEnumeratorCheckpointSerializer. */
public class JdbcEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<Collection<JdbcSourceSplit>> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<JdbcSourceSplit> checkpoint) throws IOException {
        Optional<JdbcSourceSplit> jdbcSourceSplitOptional = checkpoint.stream().findFirst();
        if (jdbcSourceSplitOptional.isPresent()) {
            Preconditions.checkArgument(
                    jdbcSourceSplitOptional.get().getClass() == JdbcSourceSplit.class,
                    "cannot serialize subclasses.");

            // since every query in splits are the same,
            // we can serialize one query only
            int size =
                    jdbcSourceSplitOptional.get().getQuery().length()
                            + (checkpoint.size() * 36)
                            + 4;
            final DataOutputSerializer out = new DataOutputSerializer(size);

            out.writeInt(checkpoint.size());
            out.writeUTF(jdbcSourceSplitOptional.get().getQuery());

            for (JdbcSourceSplit split : checkpoint) {
                out.writeUTF(split.splitId());
            }
            return out.getCopyOfBuffer();
        } else {
            throw new IOException("Cannot serialize given empty checkpoint splits.");
        }
    }

    @Override
    public Collection<JdbcSourceSplit> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        int num = in.readInt();
        String query = in.readUTF();
        final ArrayList<JdbcSourceSplit> result = new ArrayList<>(num);
        for (int remaining = num; remaining > 0; remaining--) {
            result.add(new JdbcSourceSplit(query, in.readUTF()));
        }

        return result;
    }
}
