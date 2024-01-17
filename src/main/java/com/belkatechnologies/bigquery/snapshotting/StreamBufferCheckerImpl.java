package com.belkatechnologies.bigquery.snapshotting;

import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamBufferCheckerImpl implements StreamBufferChecker {

    private final BigQueryManager bq;

    @Override
    public void waitStreamBuffer(String dataset, String tableName) {
        while (!isBufferEmpty(dataset, tableName)) {
            log.info("Table {} has not empty stream buffer. Need wait.", dataset + "." + tableName);
            sleep(30_000);
        }
    }

    @Override
    public boolean isBufferEmpty(String dataset, String table) {
        Table tableInfo = bq.getBigQuery().getTable(dataset, table);
        if (tableInfo == null) {
            return true;
        }

        StandardTableDefinition definition = tableInfo.getDefinition();
        StandardTableDefinition.StreamingBuffer streamingBuffer = definition.getStreamingBuffer();
        if (streamingBuffer == null) {
            return true;
        }
        Long estimatedRows = streamingBuffer.getEstimatedRows();
        return estimatedRows == null || estimatedRows == 0;
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
        }
    }
}
