package com.belkatechnologies.bigquery.snapshotting;

import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BqSnapshotDaoImpl implements BqSnapshotDao {

    private static final String SNAPSHOT_TEMPLATE_QUERY = "CREATE SNAPSHOT TABLE %s.%s.%s " +
            "CLONE %s.%s.%s " +
            "  OPTIONS (  expiration_timestamp = TIMESTAMP '%s');";

    private static final String SNAPSHOT_INSERT_LOG = "INSERT INTO %s.log_snapshot (postfix, timestamp) VALUES (%d, CURRENT_TIMESTAMP()) ";

    private final BigQueryManager bq;

    @Override
    public void createSnapshot(String project, String dataset, String tableName, long tablePostfix, LocalDateTime expirationDate) throws InterruptedException {
        String expirationDateFormat = expirationDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String query = String.format(SNAPSHOT_TEMPLATE_QUERY,
                project,
                BqSnapshotDao.getBackupDatasetName(dataset),
                tableName + "_" + tablePostfix,
                project,
                dataset,
                tableName,
                expirationDateFormat);
        bq.queryBatch(query);
    }

    @Override
    public void logSnapshotInfo(String devSchemaName, long postfix) {
        String query = String.format(SNAPSHOT_INSERT_LOG, devSchemaName, postfix);
        bq.query(query);
    }
}
