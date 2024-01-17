package com.belkatechnologies.bigquery.streaming.hook;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code PostAppendHook} interface defines a hook to be executed after the completion
 * of an append operation in the BigQuery streaming process.
 * Implementations of this interface can perform custom actions or processing based on the
 * result of the append operation.
 * May have multiple implementations.
 */
public interface PostAppendHook {

    /**
     * This method is called after the completion of an append operation in the BigQuery streaming process.
     *
     * @param callback       The {@code ApiFuture} representing the result of the append operation of one bathed(if possible) JSONArray
     * @param processedRows  An {@code AtomicLong} containing the count of processed rows during the operation.
     */
    void postAppendAction(ApiFuture<AppendRowsResponse> callback, AtomicLong processedRows);
}
