package com.belkatechnologies.bigquery.streaming;

import com.belkatechnologies.bigquery.configuration.BigQueryProperties;
import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.belkatechnologies.bigquery.utils.ShutDownUtils;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectFactory;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class DefaultStreamingManager implements StreamingManager {

    private final BigQueryProperties bigQueryProperties;
    private final ObjectFactory<BigQueryStreamProcessor> streamProcessorFactory;

    private final Map<TableName, BigQueryStreamProcessor> streams = new ConcurrentHashMap<>();

    private ScheduledExecutorService executorService;

    int delay;

    @PostConstruct
    private void init() {
        Integer asyncStreamingDelay = bigQueryProperties.getStreaming().getAsyncStreamingDelay();
        delay = asyncStreamingDelay != null ? asyncStreamingDelay : 30;
        Integer poolSize = bigQueryProperties.getStreaming().getStreamingManagerPoolSize();
        executorService = Executors.newScheduledThreadPool(poolSize != null ? poolSize : 200,
                new ThreadFactoryBuilder().setNameFormat("streaming-manager-%d").build());
    }

    @Override
    public void createStreamProcessor(TableName tableName) {
        streams.computeIfAbsent(tableName, table -> {
            log.debug("createStreamProcessor for table {}", table);
            BigQueryStreamProcessor streamProcessor = streamProcessorFactory.getObject();
            streamProcessor.initialize(tableName);
            executorService.scheduleWithFixedDelay(streamProcessor, 2, delay, TimeUnit.SECONDS);
            return streamProcessor;
        });
    }

    @Override
    public BigQueryStreamProcessor getStandaloneStreamProcessor(TableName tableName) {
        return streamProcessorFactory.getObject().initialize(tableName);
    }

    @Override
    public void executeOnceOnStandalone(TableName tableName, Consumer<BigQueryStreamProcessor> consumer) {
        try (var processor = getStandaloneStreamProcessor(tableName)) {
            consumer.accept(processor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void executeOnceOnStandalone(TableName tableName, Consumer<BigQueryStreamProcessor> consumer, Consumer<Exception> onException) {
        try (var processor = getStandaloneStreamProcessor(tableName)) {
            consumer.accept(processor);
        } catch (Exception e) {
            onException.accept(e);
        }
    }

    @Override
    public void putBatchForTable(TableName tableName, Collection<Map<String, Object>> batch) {
        createStreamProcessor(tableName);
        getOrThrow(tableName).putBatch(batch);
    }

    @Override
    public void putRowForTable(TableName tableName, Map<String, Object> row) {
        createStreamProcessor(tableName);
        getOrThrow(tableName).putOne(row);
    }

    @Override
    public void forceFlushStreamForTable(TableName tableName) {
        getOrThrow(tableName).forceFlush();
    }

    @Override
    public void forceFlushAll() {
        ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("forceFlushAll-%d").build());
        streams.values().forEach(stream -> threadPool.submit(stream::forceFlush));
        ShutDownUtils.shutdownWithAwait(threadPool, 10, TimeUnit.MINUTES, "forceFlushAllPool");
    }

    @Override
    public void flushStreamAndClose(TableName tableName) {
        try {
            getOrThrow(tableName).close();
            streams.remove(tableName);
        } catch (Exception e) {
            log.error("error while closing stream for table {}", tableName.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushStreamAndReinitialize(TableName tableName) {
        log.debug("Reloading stream for table {}", tableName);
        executeOnProcessorIfPresent(tableName, streamProcessor -> {
            streamProcessor.forceFlush();
            streamProcessor.initialize(tableName);
        });
        log.debug("Stream for table {} reloaded", tableName);
    }

    @Override
    public Map<TableName, StreamingStatistic> getStatistics() {
        final Map<TableName, StreamingStatistic> streamsStats = new HashMap<>();
        for (BigQueryStreamProcessor streamProcessor : streams.values()) {
            streamsStats.put(streamProcessor.getTable(), StreamingStatistic.builder()
                    .name(streamProcessor.getTable().getTable())
                    .rowQueueSize(streamProcessor.getRowQueueSize())
                    .fallBackQueueSize(streamProcessor.getFallBackQueueSize())
                    .isInitialized(streamProcessor.isInitialized())
                    .isStopped(streamProcessor.isStopped())
                    .build());
        }
        return streamsStats;
    }

    @PreDestroy
    private void destroy() {
        log.info("destroy DefaultStreamingManager");
        try {
            for (BigQueryStreamProcessor streamProcessor : streams.values()) {
                streamProcessor.close();
            }
            ShutDownUtils.shutdownWithAwait(executorService, 10, TimeUnit.MINUTES, "destroyPool");
        } catch (Exception e) {
            log.error("error while destroy DefaultStreamingManager");
        }
    }

    private BigQueryStreamProcessor getOrThrow(TableName tableName) {
        BigQueryStreamProcessor streamProcessor = streams.get(tableName);
        if (streamProcessor == null) {
            throw new RuntimeException(String.format("streamProcessor for table %s has not been created", tableName));
        }
        return streamProcessor;
    }

    private void executeOnProcessorIfPresent(TableName tableName, Consumer<BigQueryStreamProcessor> consumer) {
        BigQueryStreamProcessor streamProcessor = streams.get(tableName);
        if (streamProcessor != null) {
            consumer.accept(streamProcessor);
        } else {
            log.warn("Managed streamProcessor for table {} not found", tableName.getTable());
        }
    }
}
