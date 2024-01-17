package com.belkatechnologies.bigquery.streaming;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class StreamingStatistic {
    private final String name;
    private final int rowQueueSize;
    private final int fallBackQueueSize;
    private boolean isInitialized;
    private boolean isStopped;
}
