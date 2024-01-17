package com.belkatechnologies.bigquery.snapshotting;

import java.time.LocalDateTime;

/**
 * Interface defining operations for managing snapshots in Google Cloud BigQuery.
 */
public interface BqSnapshotDao {

    /**
     * Creates a snapshot of the specified table in BigQuery with the given project, dataset, table name,
     * table postfix, and expiration date.
     *
     * @param project        The BigQuery project name.
     * @param dataset        The dataset containing the table.
     * @param tableName      The name of the table to create a snapshot of.
     * @param tablePostfix   The postfix to be appended to the snapshot table name.
     * @param expirationDate The expiration date of the snapshot.
     * @throws InterruptedException If the operation is interrupted.
     */
    void createSnapshot(String project, String dataset, String tableName, long tablePostfix, LocalDateTime expirationDate) throws InterruptedException;

    /**
     * Logs information about a snapshot, typically used for tracking purposes.
     *
     * @param devSchemaName The name of the development schema.
     * @param postfix       The postfix associated with the snapshot.
     */
    void logSnapshotInfo(String devSchemaName, long postfix);

    /**
     * Gets the backup dataset name based on the original dataset name.
     *
     * @param dataset The original dataset name.
     * @return The backup dataset name.
     */
    static String getBackupDatasetName(String dataset) {
        return dataset + "_backup";
    }
}

