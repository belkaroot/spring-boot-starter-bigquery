package com.belkatechnologies.bigquery.streaming.processor;

import com.belkatechnologies.bigquery.streaming.callback.DefaultAbstractAppendCompleteCallback;
import com.belkatechnologies.bigquery.streaming.hook.PostAppendHook;
import com.belkatechnologies.bigquery.streaming.hook.PreAppendHook;
import com.belkatechnologies.bigquery.streaming.hook.StreamFailedHook;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.belkatechnologies.bigquery.streaming.StreamingConsts.CALLBACK_EXECUTOR;
import static com.belkatechnologies.bigquery.streaming.StreamingConsts.MAX_BYTES;

@Slf4j
@RequiredArgsConstructor
public class DefaultAsyncContinuousRetriableStreamProcessor implements BigQueryStreamProcessor {

    private final BigQueryWriteClient client;
    private final ObjectProvider<DefaultAbstractAppendCompleteCallback> callbackProvider;

    @Autowired(required = false)
    private List<PreAppendHook> preAppendHooks = new ArrayList<>();
    @Autowired(required = false)
    private List<PostAppendHook> postAppendHooks = new ArrayList<>();
    @Autowired(required = false)
    private List<StreamFailedHook> streamFailedHooks = new ArrayList<>();

    protected final Queue<JSONObject> queue = new ConcurrentLinkedQueue<>();
    protected final Queue<StreamingObject> fallBackQueue = new ConcurrentLinkedQueue<>();

    protected JsonStreamWriter streamWriter;
    protected TableName tableName;

    protected ConcurrentMap<Integer, Integer> reconnectCount = new ConcurrentHashMap<>();

    @Getter
    private boolean initialized = false;
    private final AtomicBoolean stopped = new AtomicBoolean(true);

    //на случай если в очередь быстро поступает много батчей(быстрее флаша в цикле while)
    private final static int MAX_PHASER_PARTIES = 64;

    @Override
    public DefaultAsyncContinuousRetriableStreamProcessor initialize(TableName tableName) {
        try {
            this.tableName = tableName;
            if (streamWriter != null) {
                streamWriter.close();
            }
            streamWriter = JsonStreamWriter.newBuilder(tableName.toString(), client)
                    .setExecutorProvider(
                            FixedExecutorProvider.create(Executors.newScheduledThreadPool(100, new ThreadFactoryBuilder().setNameFormat("executor-provider-%d").build())))
                    .setChannelProvider(
                            BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                    .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(1))
                                    .setKeepAliveTimeout(org.threeten.bp.Duration.ofMinutes(1))
                                    .setKeepAliveWithoutCalls(true)
                                    .build())
                    .setEnableConnectionPool(true)
                    .build();
            initialized = true;
            stopped.set(false);
            return this;
        } catch (Exception e) {
            log.error("Exception while initializing stream for table {}", tableName.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isStopped() {
        return stopped.get();
    }

    @Override
    public TableName getTable() {
        return tableName;
    }

    @Override
    public void run() {
        if (stopped.get()) {
            log.error("can not run while is stopped");
            return;
        }
        if (initialized) {
            stream();
        } else {
            throw new RuntimeException("Cant start StreamProcessor before initialization!");
        }
    }

    private void stream() {
        if (!isQueueEmpty()) {
            log.info("Flush stream for table: {}, rowQueueSize: {}, fallBackQueueSize: {}", tableName.getTable(), getRowQueueSize(), getFallBackQueueSize());
            final AtomicLong processedRows = new AtomicLong();
            final AtomicLong processedBytes = new AtomicLong();
            long start = System.currentTimeMillis();
            Phaser phaser = new Phaser(1);
            StreamingObject streamingObject = null;
            try {
                while (!isQueueEmpty()) {
                    streamingObject = poll();
                    JSONArray batch = streamingObject.jsonBatch();
                    if (batch.isEmpty()) break;
                    preAppendHooks.forEach(it -> it.preAppendAction(tableName.getTable(), batch));
                    final ApiFuture<AppendRowsResponse> responseApiFuture = streamWriter.append(batch);
                    phaser.register();
                    final var callback = callbackProvider.getObject(this, streamingObject, phaser, processedRows, processedBytes);
                    ApiFutures.addCallback(responseApiFuture, callback, CALLBACK_EXECUTOR);
                    postAppendHooks.forEach(it -> it.postAppendAction(responseApiFuture, processedRows));
                    if (phaser.getRegisteredParties() > MAX_PHASER_PARTIES) {
                        break;
                    }
                }
                try {
                    phaser.awaitAdvanceInterruptibly(phaser.arrive(), 10, TimeUnit.MINUTES);
                    long duration = System.currentTimeMillis() - start;
                    log.info("{}: {} records({} bytes) flushed successfully. It takes {} millis. Approximate speed: {} rows per millisecond",
                            tableName.getTable(), processedRows, processedBytes, duration, processedRows.get() / (duration + 1));
                } catch (TimeoutException timeoutException) {
                    log.error("phaser for table {}, timed out", tableName.getTable());
                    retryBatch(streamingObject);
                }

            } catch (Exception ex) {
                if (ex instanceof Exceptions.AppendSerializationError serializationError) {
                    log.error("Serialization error for table {}; status {}; rows: {}", tableName.getTable(), serializationError.getStatus(), serializationError.getRowIndexToErrorMessage());
                    if (streamingObject != null) {
                        JSONArray rows = streamingObject.jsonBatch();
                        serializationError.getRowIndexToErrorMessage()
                                .forEach((index, s) -> log.warn("Wrong index={}; row={}", index, rows.get(index)));
                    }
                }
                StreamingObject finalStreamingObject = streamingObject;
                streamFailedHooks.forEach(it -> it.onStreamFail(ex, finalStreamingObject));
                log.error("Some Fatal Error while processing stream {}: {}", tableName.getTable(), ex.getMessage());
            }
        }
    }

    @Override
    public synchronized void putOne(Map<String, Object> row) {
        if (stopped.get()) {
            log.error("can not accept batch while force flushing");
            return;
        }
        if (CollectionUtils.isEmpty(row)) {
            log.error("row can not be null or empty");
            return;
        }
        JSONObject json = getJsonObject(row);
        queue.add(json);
    }

    @Override
    public synchronized void putBatch(Collection<Map<String, Object>> rows) {
        if (stopped.get()) {
            log.error("can not accept batch while force flushing");
            return;
        }
        if (CollectionUtils.isEmpty(rows)) {
            log.error("rows can not be null or empty");
            return;
        }
        rows.forEach(row -> {
            if (!CollectionUtils.isEmpty(row)) {
                JSONObject json = getJsonObject(row);
                queue.add(json);
            } else {
                log.error("row can not be null or empty");
            }
        });
    }

    @Override
    public synchronized void forceFlush() {
        try {
            stopped.set(true);
            while (!isQueueEmpty()) {
                log.info("forceFlush table {}, queue size {}, failBackQueueSize {}", tableName.getTable(), getRowQueueSize(), getFallBackQueueSize());
                stream();
            }
        } catch (Exception e) {
            log.debug("Ignoring error while forceFlush", e);
        } finally {
            stopped.set(false);
        }
    }

    @Override
    public void retryBatch(StreamingObject batch) {
        if (batch == null) return;
        int key = batch.toString().hashCode();
        int retryCounts = reconnectCount.merge(key, 1, Integer::sum);
        if (retryCounts < 3) {
            addToFailBackQueue(batch);
        } else if (retryCounts == 3) {
            restoreConnection();
            addToFailBackQueue(batch);
        } else {
            log.error("Can't save batch, skip " + batch.jsonBatch().length() + " events for " + tableName.getTable());
            reconnectCount.remove(key);
        }
    }

    public void addToFailBackQueue(StreamingObject batch) {
        log.debug("Add batch to fail back queue");
        fallBackQueue.add(batch);
    }

    private JSONObject getJsonObject(Map<String, Object> row) {
        var json = new JSONObject();
        row.forEach(json::put);
        return json;
    }

    private boolean isQueueEmpty() {
        return getRowQueueSize() == 0 && getFallBackQueueSize() == 0;
    }

    @SneakyThrows
    protected void restoreConnection() {
        log.debug("Restore connection");
        close();
        while (true) {
            log.debug("Try restore connection");
            try {
                initialize(tableName);
                break;
            } catch (Exception e) {
                log.error("Unable to restore connection", e);
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            }
        }
    }

    @Override
    public int getRowQueueSize() {
        return queue.size();
    }

    @Override
    public int getFallBackQueueSize() {
        return fallBackQueue.size();
    }

    private StreamingObject poll() {
        if (!fallBackQueue.isEmpty()) {
            return fallBackQueue.poll();
        } else {
            return getSized();
        }
    }

    private StreamingObject getSized() {
        JSONArray jsonBatch = new JSONArray();
        int size = 0;
        while (size <= MAX_BYTES) {
            JSONObject poll = queue.poll();
            if (poll != null) {
                size += StreamingUtils.getSize(poll);
                jsonBatch.put(poll);
            } else {
                break;
            }
        }
        log.info("Collected batch size {} bytes", size);
        return new StreamingObject(size, jsonBatch);
    }

    @Override
    public void close() {
        forceFlush();
        stopped.set(true);
        client.close();
        streamWriter.close();
    }

    @PreDestroy
    private void destroy() {
        try {
            forceFlush();
            stopped.set(true);
            close();
        } catch (Exception ignore) {
            log.error("Error while destroy stream processor for table {}", tableName.getTable());
        }
    }
}
