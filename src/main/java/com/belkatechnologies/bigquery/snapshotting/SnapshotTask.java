package com.belkatechnologies.bigquery.snapshotting;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

@Slf4j
@RequiredArgsConstructor
public class SnapshotTask implements Runnable {

    private final StreamBufferChecker streamBufferChecker;
    private final BqSnapshotDao bqSnapshotDao;
    private final SnapshotStatistics statistics;
    private final String project;
    private final String dataset;
    private final String tableName;
    private final long snapshotPostfix;
    private final int expirationDays;
    private final boolean waitBuffer;

    @Override
    public void run() {
        if (waitBuffer) {
            streamBufferChecker.waitStreamBuffer(dataset, tableName);
        }
        createSnapshotForTable(dataset, tableName, snapshotPostfix);
    }

    private void createSnapshotForTable(String dataset, String tableName, long snapshotPostfix) {
        Instant start = Instant.now();
        log.info("Create snapshot for {}.{}", dataset, tableName);
        try {
            bqSnapshotDao.createSnapshot(project, dataset, tableName, snapshotPostfix, getExpirationDate());
        } catch (Exception e) {
            log.error("Exception while createSnapshotForTable " + dataset + "." + tableName, e);
            statistics.onSnapshotFailed(tableName);
            return;
        }
        log.info("Created snapshot for {}.{}, took {} seconds", dataset, tableName, ChronoUnit.SECONDS.between(start, Instant.now()));
        statistics.onOneSnapshotDone();
    }

    private LocalDateTime getExpirationDate() {
        return LocalDate.now(ZoneId.of("UTC"))
                .atStartOfDay()
                .plusDays(expirationDays);
    }
}