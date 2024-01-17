package com.belkatechnologies.bigquery.snapshotting;

/**
 * Interface defining operations to check and wait for stream buffers in Google Cloud BigQuery.
 */
public interface StreamBufferChecker {

    /**
     * Waits for the stream buffer associated with the specified dataset and table to be empty.
     *
     * @param dataset The name of the dataset.
     * @param tableName The name of the table.
     */
    void waitStreamBuffer(String dataset, String tableName);

    /**
     * Checks if the stream buffer associated with the specified dataset and table is empty.
     *
     * @param dataset The name of the dataset.
     * @param table The name of the table.
     * @return True if the stream buffer is empty, false otherwise.
     */
    boolean isBufferEmpty(String dataset, String table);
}
