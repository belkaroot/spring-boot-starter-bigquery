package com.belkatechnologies.bigquery.streaming.hook;

import com.belkatechnologies.bigquery.streaming.processor.StreamingObject;

/**
 * The {@code StreamFailedHook} interface defines a hook to be executed when an error occurs
 * during the processing of a streaming object in the BigQuery streaming process.
 * Implementations of this interface can handle and respond to failures in the streaming process.
 */
public interface StreamFailedHook {

    /**
     * This method is called when a fatal error occurs during the processing of a streaming object
     * in the BigQuery streaming process.
     *
     * @param batch            The exception representing the failure during the processing of the streaming object.
     * @param streamingObject  The {@code StreamingObject} that encountered the failure.
     */
    void onStreamFail(Exception batch, StreamingObject streamingObject);
}
