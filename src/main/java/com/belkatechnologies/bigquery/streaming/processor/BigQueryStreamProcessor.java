package com.belkatechnologies.bigquery.streaming.processor;

import com.google.cloud.bigquery.storage.v1.TableName;

import java.util.Collection;
import java.util.Map;

/**
 * Interface defining operations for processing streaming data into Google Cloud BigQuery.
 */
public interface BigQueryStreamProcessor extends AutoCloseable, Runnable {

    /**
     * Flushes all pending batches. Ignores pending batches waiting to retry (in the fallback queue).
     * BigQueryStreamProcessor does not accept new batches while force flushing is being performed.
     */
    void forceFlush();

    /**
     * Initializes the BigQueryStreamProcessor for the assigned table.
     *
     * @param tableName The BigQuery object representing the table (project, dataset, table).
     * @return The initialized BigQueryStreamProcessor instance.
     */
    BigQueryStreamProcessor initialize(TableName tableName);

    /**
     * Checks if the BigQueryStreamProcessor is stopped.
     *
     * @return True if the processor is stopped, false otherwise.
     */
    boolean isStopped();

    /**
     * Checks if the BigQueryStreamProcessor is initialized.
     *
     * @return True if the processor is initialized, false otherwise.
     */
    boolean isInitialized();

    /**
     * Gets the BigQuery table associated with the processor.
     *
     * @return The TableName representing the BigQuery table.
     */
    TableName getTable();

    /**
     * Adds a single row to the processing queue.
     *
     * @param row The map representing the row data.
     */
    void putOne(Map<String, Object> row);

    /**
     * Adds a batch of rows to the processing queue.
     *
     * @param rows The collection of maps representing the rows.
     */
    void putBatch(Collection<Map<String, Object>> rows);

    /**
     * Retries processing a failed batch by adding it to the fallback queue.
     *
     * @param batch The failed batch to retry.
     */
    void retryBatch(StreamingObject batch);

    /**
     * Gets the size of the row processing queue.
     *
     * @return The size of the row processing queue.
     */
    int getRowQueueSize();

    /**
     * Gets the size of the fallback queue containing batches waiting to be retried.
     *
     * @return The size of the fallback queue.
     */
    int getFallBackQueueSize();
}
