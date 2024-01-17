package com.belkatechnologies.bigquery.utils;

import lombok.SneakyThrows;

import java.util.List;

/**
 * Interface for retrieving metadata information from Google Cloud BigQuery.
 */
public interface MetadataService {

    /**
     * Gets the list of table names for a given project and schema.
     *
     * @param project    The Google Cloud project name.
     * @param schemaName The schema or dataset name.
     * @return A list of table names.
     */
    List<String> getTables(String project, String schemaName);

    /**
     * Gets the list of table snapshots for a given project, schema, and table name.
     *
     * @param project    The Google Cloud project name.
     * @param schemaName The schema or dataset name.
     * @param tableName  The name of the table.
     * @return A list of table snapshots.
     */
    @SneakyThrows
    List<String> getTableSnapshots(String project, String schemaName, String tableName);

    /**
     * Gets metadata information for a given project, schema, and table name.
     *
     * @param project    The Google Cloud project name.
     * @param schemaName The schema or dataset name.
     * @param tableName  The name of the table.
     * @return A list of {@link Column} objects representing table metadata.
     */
    @SneakyThrows
    List<Column> getTableMetaData(String project, String schemaName, String tableName);
}
