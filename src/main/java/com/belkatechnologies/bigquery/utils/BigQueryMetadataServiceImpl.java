package com.belkatechnologies.bigquery.utils;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableResult;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


@Slf4j
@Component
@RequiredArgsConstructor
public class BigQueryMetadataServiceImpl implements MetadataService {

    private final BigQuery bq;

    @SneakyThrows
    @Override
    public List<String> getTables(String project, String schemaName) {
        final String TABLES = "select table_name from %s.%s.INFORMATION_SCHEMA.TABLES where table_type=\"BASE TABLE\";";
        final String query = String.format(TABLES, project, schemaName);
        final TableResult result = bq.query(BqQueryUtils.createQuery(query));
        return StreamSupport.stream(result.getValues().spliterator(), false)
                .map(field -> field.get(0).getStringValue())
                .collect(Collectors.toList());
    }

    @SneakyThrows
    @Override
    public List<String> getTableSnapshots(String project, String schemaName, String tableName) {
        final String snapshots = "SELECT * FROM %s.%s.INFORMATION_SCHEMA.TABLE_SNAPSHOTS WHERE base_table_name=\"%s\";";
        final String query = String.format(snapshots, project, schemaName, tableName);
        final TableResult result = bq.query(BqQueryUtils.createQuery(query));
        return StreamSupport.stream(result.getValues().spliterator(), false)
                .map(field -> field.get(2).getStringValue())
                .collect(Collectors.toList());
    }

    @SneakyThrows
    @Override
    public List<Column> getTableMetaData(String project, String schemaName, String tableName) throws SQLException {
        final String TABLES = "select table_name, column_name, data_type from %s.%s.INFORMATION_SCHEMA.COLUMNS where table_name = '%s';";
        final String query = String.format(TABLES, project, schemaName, tableName);
        final TableResult result = bq.query(BqQueryUtils.createQuery(query));
        return result.streamAll()
                .map(field -> new Column()
                        .setName(field.get("column_name").getStringValue())
                        .setType(field.get("data_type").getStringValue()))
                .collect(Collectors.toList());
    }
}
