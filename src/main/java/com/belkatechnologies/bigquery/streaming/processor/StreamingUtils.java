package com.belkatechnologies.bigquery.streaming.processor;

import com.belkatechnologies.bigquery.streaming.model.StreamBatch;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;

import static com.belkatechnologies.bigquery.streaming.StreamingConsts.MAX_BYTES;
import static com.google.protobuf.CodedOutputStream.*;

@Slf4j
public class StreamingUtils {

    public static int getSize(JSONArray jsonArray) {
        int result = 0;
        for (Object jsonObject : jsonArray) {
            result = result + getSize((JSONObject) jsonObject);
        }
        return result;
    }

    public static int getSize(JSONObject jsonObject) {
        int result = 0;
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);
            if (value instanceof ByteString) {
                result = result + computeBytesSize(key.length(), (ByteString) value);
            } else if (value instanceof String) {
                result = result + computeStringSize(key.length(), (String) value);
            } else if (value instanceof Long) {
                result = result + computeInt64Size(key.length(), (Long) value);
            } else if (value instanceof Integer) {
                result = result + computeInt64Size(key.length(), (Integer) value);
            } else if (value instanceof Boolean) {
                result = result + computeBoolSize(key.length(), (Boolean) value);
            } else if (value instanceof Float) {
                result = result + computeFloatSize(key.length(), (Float) value);
            } else if (value instanceof Double) {
                result = result + computeDoubleSize(key.length(), (Double) value);
            } else {
                log.warn("Got unknown type {}, key {}, value {}", value.getClass(), key, value);
                result = result + computeStringSize(key.length(), value.toString());
            }
        }
        return result;
    }

    /**
     * split into several batches while all of them is not less than MAX_BYTES
     */
    @Deprecated
    public static List<JSONArray> optimizeBatchSize(JSONArray records) {
        List<JSONArray> result = new ArrayList<>();
        if (records.toString().length() > MAX_BYTES && records.length() > 1) {
            int middleOfBatch = records.length() / 2;
            result.addAll(optimizeBatchSize(getPart(records, 0, middleOfBatch)));
            result.addAll(optimizeBatchSize(getPart(records, middleOfBatch, records.length())));
        } else {
            result.add(records);
        }
        return result;
    }

    @Deprecated
    private static JSONArray getPart(JSONArray jsonArray, int from, int count) {
        return StreamSupport
                .stream(jsonArray.spliterator(), false)
                .skip(from)
                .limit(count)
                .collect(Collector.of(
                        JSONArray::new,
                        JSONArray::put,
                        JSONArray::put
                ));
    }

    @Deprecated
    public static List<StreamBatch> optimizeBatchSize(StreamBatch batch) {
        List<StreamBatch> result = new ArrayList<>();
        if (getBatchBytesSize(batch) <= MAX_BYTES) {
            result.add(batch);
        } else {
            boolean optimized = false;
            Queue<StreamBatch> queue = new ArrayDeque<>();
            queue.add(batch);
            while (!optimized) {
                boolean oneOk = false;
                boolean twoOk = false;
                StreamBatch poll = queue.poll();
                StreamBatch half1 = StreamBatch.builder()
                        .tableName(poll.getTableName())
                        .eventName(poll.getEventName())
                        .userId(poll.getUserId())
                        .batchType(poll.getBatchType())
                        .rows(List.copyOf(poll.getRows().subList(0, poll.getRows().size() / 2)))
                        .build();
                StreamBatch half2 = StreamBatch.builder()
                        .tableName(poll.getTableName())
                        .eventName(poll.getEventName())
                        .userId(poll.getUserId())
                        .batchType(poll.getBatchType())
                        .rows(List.copyOf(poll.getRows().subList(poll.getRows().size() / 2, poll.getRows().size())))
                        .build();
                if (getBatchBytesSize(half1) <= MAX_BYTES) {
                    oneOk = true;
                    result.add(half1);
                } else {
                    queue.add(half1);
                }
                if (getBatchBytesSize(half2) <= MAX_BYTES) {
                    twoOk = true;
                    result.add(half2);
                } else {
                    queue.add(half2);
                }
                if (oneOk && twoOk) {
                    optimized = true;
                }
            }
        }
        return result;
    }

    @Deprecated
    public static int getBatchBytesSize(JSONArray batch) {
        return batch.toString().length();
    }

    @Deprecated
    public static int getBatchBytesSize(StreamBatch batch) {
        return new JSONArray(batch.getRows()).toString().length();
    }
}
