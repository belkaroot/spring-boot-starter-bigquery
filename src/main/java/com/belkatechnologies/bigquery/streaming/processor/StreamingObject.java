package com.belkatechnologies.bigquery.streaming.processor;

import org.json.JSONArray;

/**
 * Record representing a streaming object containing size information and a JSON batch.
 *
 * @param size      The size of the streaming object in bytes.
 * @param jsonBatch The JSON batch associated with the streaming object.
 */
public record StreamingObject(int size, JSONArray jsonBatch) {
}
