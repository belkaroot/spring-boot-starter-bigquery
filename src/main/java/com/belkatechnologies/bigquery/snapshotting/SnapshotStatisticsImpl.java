package com.belkatechnologies.bigquery.snapshotting;

import org.apache.commons.lang3.tuple.MutablePair;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SnapshotStatisticsImpl implements SnapshotStatistics {

    private final MutablePair<AtomicInteger, Integer> snapshotProgress = MutablePair.of(new AtomicInteger(), 0);
    private final Set<String> failedSnapshots = new HashSet<>();

    @Override
    public void onSnapshotStarted(Integer tablesToSnapshot) {
        snapshotProgress.setRight(tablesToSnapshot);
        failedSnapshots.clear();
    }

    @Override
    public void onOneSnapshotDone() {
        snapshotProgress.getLeft().incrementAndGet();
    }

    @Override
    public void onSnapshotFinished() {
        snapshotProgress.getLeft().set(0);
        snapshotProgress.setRight(0);
    }

    @Override
    public void onSnapshotFailed(String table) {
        failedSnapshots.add(table);
    }

    @Override
    public boolean hasFails() {
        return !failedSnapshots.isEmpty();
    }

    @Override
    public MutablePair<AtomicInteger, Integer> getCurrentStat() {
        return snapshotProgress;
    }

    @Override
    public Set<String> getFailed() {
        return failedSnapshots;
    }
}
