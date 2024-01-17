package com.belkatechnologies.bigquery.streaming;

import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.google.cloud.bigquery.storage.v1.TableName;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Interface defining operations for managing streaming data to Google Cloud BigQuery.
 */
public interface StreamingManager {

    /**
     * Creates a BigQueryStreamProcessor instance for the provided TableName and schedules it to be flushed every 30(or assigned via properties) seconds.
     * The method is idempotent and does not create a new StreamProcessor if one already exists for the TableName.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     */
    void createStreamProcessor(TableName tableName);

    /**
     * Creates and returns a standalone BigQueryStreamProcessor instance for the provided TableName.
     * This instance will not be flushed automatically and is supposed to be used like a one-time flusher.
     * It is recommended to obtain it in a try-finally manner to ensure automatic closure.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     * @return The standalone BigQueryStreamProcessor instance.
     */
    BigQueryStreamProcessor getStandaloneStreamProcessor(TableName tableName);

    /**
     * Executes the provided consumer on a standalone instance of the BigQueryStreamProcessor for the specified table.
     *
     * @param tableName The name of the table for which the standalone BigQueryStreamProcessor is created and executed.
     * @param consumer  The consumer function to be executed on the standalone BigQueryStreamProcessor.
     * @throws RuntimeException If an error occurs during the execution of the consumer.
     */
    void executeOnceOnStandalone(TableName tableName, Consumer<BigQueryStreamProcessor> consumer);

    /**
     * Executes the provided consumer on a standalone instance of the BigQueryStreamProcessor for the specified table.
     * Allows handling exceptions with a custom consumer.
     *
     * @param tableName   The name of the table for which the standalone BigQueryStreamProcessor is created and executed.
     * @param consumer    The consumer function to be executed on the standalone BigQueryStreamProcessor.
     * @param onException A consumer to handle exceptions that may occur during the execution of the provided consumer.
     */
    void executeOnceOnStandalone(TableName tableName, Consumer<BigQueryStreamProcessor> consumer, Consumer<Exception> onException);

    /**
     * Adds a batch of rows to the processing queue for the specified table.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     * @param rows      The collection of maps representing the rows.
     */
    void putBatchForTable(TableName tableName, Collection<Map<String, Object>> rows);

    /**
     * Adds a single row to the processing queue for the specified table.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     * @param row       The map representing the row data.
     */
    void putRowForTable(TableName tableName, Map<String, Object> row);

    /**
     * Forces the flushing of the stream for the specified table.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     * @see BigQueryStreamProcessor#forceFlush()
     */
    void forceFlushStreamForTable(TableName tableName);

    /**
     * Flushes all created streams, blocking the current thread while all streams are flushed.
     */
    void forceFlushAll();

    /**
     * Flushes the stream for the specified table and closes it.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     */
    void flushStreamAndClose(TableName tableName);

    /**
     * Flushes the stream for the specified table and reinitializes it.
     *
     * @param tableName The BigQuery object representing project, dataset, and table names.
     */
    void flushStreamAndReinitialize(TableName tableName);

    /**
     * Retrieves statistics for all streaming instances.
     *
     * @return A map containing statistics for each table.
     */
    Map<TableName, StreamingStatistic> getStatistics();
}

