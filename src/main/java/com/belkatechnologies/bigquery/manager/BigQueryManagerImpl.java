package com.belkatechnologies.bigquery.manager;

import com.google.cloud.bigquery.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class BigQueryManagerImpl implements BigQueryManager {

    private final BigQuery bigQuery;

    @Override
    public BigQuery getBigQuery() {
        return bigQuery;
    }

    @Override
    public TableResult query(QueryJobConfiguration queryConfig) {
        try {
            return bigQuery.query(queryConfig);
        } catch (JobException e) {
            log.error("Error while querying, JobException: {} , {}", queryConfig.getQuery(), e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.error("Error while querying, InterruptedException: {}, {}", queryConfig.getQuery(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableResult query(String query) {
        QueryJobConfiguration config = QueryJobConfiguration.of(query);
        return query(config);
    }

    @Override
    public TableResult query(String query, Map<String, QueryParameterValue> namedParameters) {
        if (namedParameters != null && !namedParameters.isEmpty()) {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                    .setNamedParameters(namedParameters)
                    .build();
            return query(queryConfig);
        } else {
            return query(query);
        }
    }

    @Override
    public TableResult queryBatch(String query) {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                // Run at batch priority, which won't count toward concurrent rate limit.
                .setPriority(QueryJobConfiguration.Priority.BATCH)
                .build();
        return query(queryConfig);
    }

    @Override
    public TableResult queryBatch(String query, Map<String, QueryParameterValue> namedParameters) {
        QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(query)
                // Run at batch priority, which won't count toward concurrent rate limit.
                .setPriority(QueryJobConfiguration.Priority.BATCH);
        if (namedParameters != null && !namedParameters.isEmpty()) {
            QueryJobConfiguration queryConfig = queryConfigBuilder
                    .setNamedParameters(namedParameters)
                    .build();
            return query(queryConfig);
        } else {
            return query(queryConfigBuilder.build());
        }
    }

    @Override
    public long update(String query) {
        TableResult tableResult = query(query);
        Job job = bigQuery.getJob(tableResult.getJobId()).reload();
        JobStatistics.QueryStatistics statistics = job.getStatistics();
        return statistics.getNumDmlAffectedRows() != null ? statistics.getNumDmlAffectedRows() : 0;
    }


    @Override
    public void query(String query, Map<String, QueryParameterValue> namedParameters, Consumer<FieldValueListDecorator> forEach) {
        for (FieldValueList value : query(query, namedParameters).iterateAll()) {
            forEach.accept(FieldValueListDecorator.of(value));
        }
    }

    @Override
    public void query(String query, Consumer<FieldValueListDecorator> forEach) {
        for (FieldValueList value : query(query).iterateAll()) {
            forEach.accept(FieldValueListDecorator.of(value));
        }
    }

    @Override
    public <T> T get(String query, Function<Iterable<FieldValueList>, T> extractor) {
        return extractor.apply(query(query).iterateAll());
    }

    @Override
    public <T> List<T> list(String query, Function<FieldValueListDecorator, T> extractor) {
        List<T> res = new ArrayList<>();
        for (FieldValueList value : query(query).iterateAll()) {
            res.add(extractor.apply(FieldValueListDecorator.of(value)));
        }
        return res;
    }

    @Override
    public <T> List<T> list(String query, Map<String, QueryParameterValue> namedParameters, Function<FieldValueListDecorator, T> extractor) {
        List<T> res = new ArrayList<>();
        for (FieldValueList value : query(query, namedParameters).iterateAll()) {
            res.add(extractor.apply(FieldValueListDecorator.of(value)));
        }
        return res;
    }

    @Override
    public void list(TableId tableId, int pageSize, Consumer<FieldValueListDecorator> forEach) {
        try {
            final TableResult result = bigQuery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(pageSize));
            for (FieldValueList value : result.iterateAll()) {
                forEach.accept(FieldValueListDecorator.of(value));
            }
        } catch (Exception ex) {
            log.error("Error while listing table data", ex);
            throw ex;
        }
    }

    @Override
    public void list(String dataset, String table, Schema schema, Consumer<FieldValueListDecorator> forEach) {
        list(dataset, table, pageSize, schema, forEach);
    }

    @Override
    public void list(String dataset, String table, int pageSize, Schema schema, Consumer<FieldValueListDecorator> forEach) {
        try {
            final TableId tableId = TableId.of(dataset, table);
            final TableResult result = bigQuery.listTableData(tableId, schema, BigQuery.TableDataListOption.pageSize(pageSize));
            for (FieldValueList value : result.iterateAll()) {
                forEach.accept(FieldValueListDecorator.of(value));
            }
        } catch (Exception ex) {
            log.error("Error while listing table data", ex);
            throw ex;
        }
    }

    @Override
    public <T> T one(String query, Function<FieldValueListDecorator, T> extractor) {
        final TableResult result = query(query);
        final long size = result.getTotalRows();
        if (size == 0) {
            throw new RuntimeException("Empty result");
        } else if (size > 1) {
            throw new RuntimeException("Result contains more then one row");
        }
        return extractor.apply(FieldValueListDecorator.of(result.iterateAll().iterator().next()));
    }

    @Override
    public <T> T one(String query, Function<FieldValueListDecorator, T> extractor, T defaultValue) {
        final TableResult result = query(query);
        final long size = result.getTotalRows();
        if (size == 0) {
            return defaultValue;
        } else if (size > 1) {
            throw new RuntimeException("Result contains more then one row");
        }
        return extractor.apply(FieldValueListDecorator.of(result.iterateAll().iterator().next()));
    }

    @Override
    public void createTableLike(String dataset, String sourceTableName, String targetTableName) {
        final String createTempTable = String.format("create or replace table %s.%s like %s.%s", dataset, targetTableName, dataset, sourceTableName);
        query(createTempTable);
    }

    @Override
    public boolean dropTable(String dataset, String tableName) {
        try {
            return getBigQuery().delete(TableId.of(dataset, tableName));
        } catch (Exception ex) {
            log.error("Error while deleting table", ex);
            throw ex;
        }
    }

    @Override
    public void renameTable(String dataset, String from, String to) {
        try {
            final String renameTable = String.format("alter table %s.%s rename to %s", dataset, from, to);
            query(renameTable);
        } catch (Exception ex) {
            log.error("Error while renaming table {}", from, ex);
            throw ex;
        }
    }

}
