package com.belkatechnologies.bigquery.snapshotting;

public interface SnapshotService {

    /**
     * Creates snapshots based on the provided {@link SnapshotConfiguration}.
     *
     * @param snapshotConfiguration The configuration for creating snapshots.
     */
    void createSnapshots(SnapshotConfiguration snapshotConfiguration);
}
