package com.belkatechnologies.bigquery.snapshotting;

import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interface defining operations for tracking statistics related to snapshots.
 */
public interface SnapshotStatistics {

    /**
     * Signals the start of a snapshot, providing the total number of tables to be snapshot.
     *
     * @param tablesToSnapshot The total number of tables to be snapshot.
     */
    void onSnapshotStarted(Integer tablesToSnapshot);

    /**
     * Signals the completion of one snapshot.
     */
    void onOneSnapshotDone();

    /**
     * Signals the completion of the entire snapshot process.
     */
    void onSnapshotFinished();

    /**
     * Signals that snapshotting failed for table completion of the entire snapshot process.
     */
    void onSnapshotFailed(String table);

    boolean hasFails();

    /**
     * Retrieves the current snapshot statistics as a mutable pair containing the number
     * of completed snapshots and the total number of tables to be snapshot.
     *
     * @return A mutable pair containing the number of completed snapshots and the total number of tables to be snapshot.
     */
    MutablePair<AtomicInteger, Integer> getCurrentStat();

    Set<String> getFailed();
}
