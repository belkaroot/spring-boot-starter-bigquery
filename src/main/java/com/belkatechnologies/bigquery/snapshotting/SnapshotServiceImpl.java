package com.belkatechnologies.bigquery.snapshotting;

import com.belkatechnologies.bigquery.configuration.BigQueryProperties;
import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.manager.BqDatasetDaoImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotServiceImpl implements SnapshotService {

    private static final String ALL_TABLES_DATASET = "select table_name from %s.%s.INFORMATION_SCHEMA.TABLES " +
            " WHERE table_type = 'BASE TABLE' ";

    private final BqSnapshotDao bqSnapshotDao;
    private final BqDatasetDaoImpl bqDatasetDaoImpl;

    private final BigQueryManager bq;

    private final StreamBufferChecker streamBufferChecker;

    private final BigQueryProperties bigQueryProperties;

    @Getter
    private final SnapshotStatistics statistics;

    @Override
    public void createSnapshots(SnapshotConfiguration snapshotConfiguration) {
        if (isDisabled()) return;
        validateConfiguration(snapshotConfiguration);
        launchSnapshotting(snapshotConfiguration);
    }

    private void launchSnapshotting(SnapshotConfiguration snapshotConfiguration) {
        boolean waitBuffer = snapshotConfiguration.waitBuffer();
        String logTableDataset = bigQueryProperties.getSnapshotting().getLogTableDataset();
        int expirationDays = snapshotConfiguration.expirationDays();

        long snapshotPostfix = System.currentTimeMillis();
        log.info("Start creating snapshot with postfix {}", snapshotPostfix);
        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("snapshot-thread-%d").build());
        AtomicInteger taskCounter = new AtomicInteger();

        snapshotConfiguration.datasets().forEach((dataset, tables) -> {
            if (CollectionUtils.isEmpty(tables)) {
                log.info("Adding all tables from dataset {} as snapshot candidates", dataset);
                int snapshotsCount = snapshotAllDataset(dataset, snapshotPostfix, waitBuffer, expirationDays, executor);
                taskCounter.addAndGet(snapshotsCount);
            } else {
                log.info("Adding specified tables from dataset {} as snapshot candidates, tables {}", dataset, tables);
                tables.forEach(table -> {
                    snapshotTable(dataset, table, snapshotPostfix, waitBuffer, expirationDays, executor);
                    taskCounter.incrementAndGet();
                });
            }
        });
        log.info("All snapshot task created");
        statistics.onSnapshotStarted(taskCounter.get());

        waitAll(executor);

        bqSnapshotDao.logSnapshotInfo(logTableDataset, snapshotPostfix);
        log.info("Finish creating snapshot {}", snapshotPostfix);
        statistics.onSnapshotFinished();
    }

    private void snapshotTable(String dataset,
                               String table,
                               long snapshotPostfix,
                               boolean waitBuffer,
                               int expirationDays,
                               ExecutorService executor) {
        executor.submit(
                new SnapshotTask(
                        streamBufferChecker,
                        bqSnapshotDao,
                        statistics,
                        bigQueryProperties.getData().getProject(),
                        dataset,
                        table,
                        snapshotPostfix,
                        expirationDays,
                        waitBuffer
                )
        );
    }

    private int snapshotAllDataset(String dataset,
                                   long snapshotPostfix,
                                   boolean waitBuffer,
                                   int expirationDays,
                                   ExecutorService executor) {
        boolean datasetExists = bqDatasetDaoImpl.exists(dataset);
        if (!datasetExists) {
            log.info("Dataset {} not exist. Skip creating snapshot", dataset);
            return 0;
        }
        String backupDatasetName = BqSnapshotDao.getBackupDatasetName(dataset);
        boolean backupDatasetExists = bqDatasetDaoImpl.exists(backupDatasetName);
        if (!backupDatasetExists) {
            log.info("Dataset {} not exist. Try create.", backupDatasetName);
            backupDatasetExists = bqDatasetDaoImpl.create(backupDatasetName);
            if (!backupDatasetExists) {
                log.error("Dataset {} not exist. Create snabshot aborted.", backupDatasetName);
                return 0;
            }
        }

        String query = String.format(ALL_TABLES_DATASET, bigQueryProperties.getData().getProject(), dataset);

        AtomicInteger taskCointer = new AtomicInteger();
        bq.query(query, rs -> {
            SnapshotTask task = new SnapshotTask(
                    streamBufferChecker,
                    bqSnapshotDao,
                    statistics,
                    bigQueryProperties.getData().getProject(),
                    dataset,
                    rs.getString(0),
                    snapshotPostfix,
                    expirationDays,
                    waitBuffer
            );
            executor.submit(task);
            taskCointer.incrementAndGet();
        });
        return taskCointer.get();
    }

    private void validateConfiguration(SnapshotConfiguration snapshotConfiguration) {
        if (snapshotConfiguration == null) {
            throw new RuntimeException("snapshotConfiguration must not be null or empty");
        }
        if (ObjectUtils.isEmpty(bigQueryProperties.getSnapshotting().getLogTableDataset())) {
            throw new RuntimeException("logTableDataset property must not be null or empty");
        }
        if (snapshotConfiguration.expirationDays() <= 0) {
            throw new RuntimeException("expirationDays property must be greater then zero");
        }
        if (snapshotConfiguration.datasets().isEmpty()) {
            throw new RuntimeException("datasets property must not be empty");
        }
    }

    private boolean isDisabled() {
        if (!bigQueryProperties.getSnapshotting().isEnabled()) {
            log.info("Creating snapshots is disabled");
            return true;
        }
        return false;
    }

    public void waitAll(ExecutorService executor) {
        log.info("Wait for all task will complete");
        executor.shutdown();
        try {
            executor.awaitTermination(180, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("Executor snapshot task shutdown");
    }
}
