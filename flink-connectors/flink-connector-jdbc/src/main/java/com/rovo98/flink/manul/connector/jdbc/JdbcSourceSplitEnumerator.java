package com.rovo98.flink.manul.connector.jdbc;

import com.rovo98.flink.manul.connector.jdbc.split.JdbcSourceSplit;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;

/**
 * Jdbc source splits enumerator which is responsible for assigning {@link JdbcSourceSplit}s to the
 * source reader.
 */
public class JdbcSourceSplitEnumerator
        implements SplitEnumerator<JdbcSourceSplit, Collection<JdbcSourceSplit>> {

    private final SplitEnumeratorContext<JdbcSourceSplit> context;
    private final Queue<JdbcSourceSplit> remainingSplits;
    private final HashMap<Integer, List<JdbcSourceSplit>> splitsForSubtasks;

    private final boolean bounded;

    public JdbcSourceSplitEnumerator(
            SplitEnumeratorContext<JdbcSourceSplit> context,
            Collection<JdbcSourceSplit> splits,
            boolean bounded) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>(splits);
        this.bounded = bounded;
        this.splitsForSubtasks = new HashMap<>(context.currentParallelism());
        this.refreshAssignedSplitsForSubtasks();
    }

    private void refreshAssignedSplitsForSubtasks() {
        this.splitsForSubtasks.clear();
        int i = 1;
        int pm = context.currentParallelism();
        for (JdbcSourceSplit split : remainingSplits) {
            int subtaskIndex = i % pm;
            splitsForSubtasks.computeIfAbsent(subtaskIndex, s -> new ArrayList<>()).add(split);
            i++;
        }
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostName) {
        List<JdbcSourceSplit> splitsForThisSubtask = splitsForSubtasks.get(subtaskId);
        if (splitsForThisSubtask != null && !splitsForThisSubtask.isEmpty()) {
            context.assignSplits(
                    new SplitsAssignment<>(
                            new HashMap<Integer, List<JdbcSourceSplit>>() {
                                {
                                    put(subtaskId, splitsForThisSubtask);
                                }
                            }));
            splitsForSubtasks.remove(subtaskId);
        } else {
            // signal no more splits (bounded) or never end (unbounded)
            if (bounded) {
                context.signalNoMoreSplits(subtaskId);
            }
        }
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
        this.remainingSplits.addAll(splits);
        this.refreshAssignedSplitsForSubtasks();
    }

    @Override
    public void addReader(int i) {}

    @Override
    public Collection<JdbcSourceSplit> snapshotState(long l) throws Exception {
        return remainingSplits;
    }

    @Override
    public void close() throws IOException {}
}
