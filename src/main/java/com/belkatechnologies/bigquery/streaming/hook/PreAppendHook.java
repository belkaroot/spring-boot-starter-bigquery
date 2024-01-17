package com.belkatechnologies.bigquery.streaming.hook;

import org.json.JSONArray;

/**
 * The {@code PreAppendHook} interface defines a hook to be executed before the initiation
 * of an append operation in the BigQuery streaming process.
 * Implementations of this interface can perform custom actions or modifications
 * to the data batch before it is appended to the BigQuery table.
 * May have multiple implementations.
 */
public interface PreAppendHook {

    /**
     * This method is called before the initiation of an append operation in the BigQuery streaming process.
     * Implementations can perform custom actions or modifications to the data batch before it is appended.
     *
     * @param table  The name of the BigQuery table to which the data batch is being appended.
     * @param batch  The {@code JSONArray} representing the data batch to be appended.
     */
    void preAppendAction(String table, JSONArray batch);
}
