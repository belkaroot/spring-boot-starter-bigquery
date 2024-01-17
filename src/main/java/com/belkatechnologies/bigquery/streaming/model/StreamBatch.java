package com.belkatechnologies.bigquery.streaming.model;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * abstraction alternative to parameter maps
 * you can store here additional metadata, such as eventName, userId, tableName
 */
@EqualsAndHashCode
@Builder
@Data
@Deprecated
public class StreamBatch {

    protected final String tableName;
    protected String eventName;
    protected String userId;
    protected final BatchType batchType;
    protected List<Map<String, Object>> rows;

    public void addRow(Map<String, Object> row) {
        rows.add(row);
    }

    public int size() {
        return rows.size();
    }
}
