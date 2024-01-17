package com.belkatechnologies.bigquery.utils;

import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.manager.FieldValueListDecorator;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Utility class for executing batch queries on Google Cloud BigQuery.
 */
@Slf4j
public class BatchQueryExecutor {
    private final String baseQuery;
    private final String countQuery;
    private final BigQueryManager bq;
    private final String name;
    private final AtomicInteger counter = new AtomicInteger();
    private final Consumer<FieldValueListDecorator> function;
    private final String countField;

    /**
     * Constructs a BatchQueryExecutor instance.
     *
     * @param countQuery  The query to get the count of rows.
     * @param baseQuery   The base query for fetching data.
     * @param bq          The BigQuery manager.
     * @param name        The name of the batch query.
     * @param countField  The field used for counting.
     * @param function    The consumer function to process fetched data.
     */
    public BatchQueryExecutor(String countQuery, String baseQuery, BigQueryManager bq, String name, String countField, Consumer<FieldValueListDecorator> function) {
        this.countQuery = countQuery;
        this.baseQuery = baseQuery;
        this.bq = bq;
        this.name = name;
        this.function = function;
        this.countField = countField;
    }

    /**
     * Fetches and processes data using batch queries.
     */
    public void fetch() {
        long rowCounts = bq.one(countQuery, values -> values.getLong("count"));
        int batchCount = Math.min(20, (int) (rowCounts / 200_000) + 1);
        ExecutorService executor = Executors.newFixedThreadPool(batchCount, new ThreadFactoryBuilder().setNameFormat("batch-query-thread-%d").build());
        log.info("Begin init {}, count {}, thread count {}", name, rowCounts, batchCount);
        for (int i = 0; i < batchCount; i++) {
            final int currentBatchNumber = i;
            executor.submit(() -> {
                log.debug("Process batch {} start {}", name, Thread.currentThread().getName());
                hardRetry(() -> execute(currentBatchNumber, batchCount), "Process data");
                log.debug("Process batch {} complete {}", name, Thread.currentThread().getName());
            });
        }

        waitAll(executor);
        log.info("Complete {}, wait {}, loaded {}", name, rowCounts, counter.get());
    }

    private String getQuery(int currentBatchNumber, int batchCount) {
        if (baseQuery.toLowerCase().contains("where")) {
            return baseQuery + " and MOD(" + countField + ", " + batchCount + ") = " + currentBatchNumber;
        } else {
            return baseQuery + " where MOD(" + countField + ", " + batchCount + ") = " + currentBatchNumber;
        }
    }

    private Boolean execute(int currentBatchNumber, int batchCount) {
        String query = getQuery(currentBatchNumber, batchCount);
        TableResult tableResult = hardRetry(() -> bq.query(query), "Execute query");
        Iterable<FieldValueList> fieldValueLists = tableResult.iterateAll();
        for (FieldValueList fieldValues : fieldValueLists) {
            FieldValueListDecorator rs = FieldValueListDecorator.of(fieldValues);
            function.accept(rs);
            if (counter.getAndIncrement() % 1_000_000 == 0) {
                log.debug("Init {}: {}", name, counter);
            }
        }
        return true;
    }

    // Retry method in case of exceptions, such as Premature EOF from BigQuery
    @Nullable
    private <T> T hardRetry(Callable<T> s, String operationName) {
        int retryCount = 0;
        while (true) {
            try {
                return s.call();
            } catch (Exception e) {
                retryCount++;
                log.error("Cant do {} for {}. Thread {}, try {} time; ", operationName, name, Thread.currentThread().getName(), retryCount, e);
                sleep();
            }
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void waitAll(ExecutorService executor) {
        log.info("Wait for {} complete", name);
        executor.shutdown();
        try {
            executor.awaitTermination(180, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("Executor fetch {} shutdown", name);
    }
}
