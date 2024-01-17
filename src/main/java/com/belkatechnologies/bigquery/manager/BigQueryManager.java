package com.belkatechnologies.bigquery.manager;

import com.google.cloud.bigquery.*;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Interface defining methods for managing interactions with Google Cloud BigQuery.
 * @author Ilia Guzenko, Denis Chernyshev
 */
public interface BigQueryManager {

    /**
     * Default page size for queries.
     */
    int pageSize = 10000;

    /**
     * Gets the BigQuery instance.
     *
     * @return The BigQuery instance.
     */
    BigQuery getBigQuery();

    /**
     * Executes a query job with the specified configuration.
     *
     * @param queryConfig The configuration for the query job.
     * @return The result of the query job.
     * @throws InterruptedException If the query job is interrupted.
     */
    TableResult query(QueryJobConfiguration queryConfig) throws InterruptedException;

    /**
     * Executes a query with the specified SQL string.
     *
     * @param query The SQL query string.
     * @return The result of the query.
     */
    TableResult query(String query);

    /**
     * Executes a parameterized query with the specified SQL string and named parameters.
     *
     * @param query           The SQL query string.
     * @param namedParameters Named parameters for the query.
     * @return The result of the parameterized query.
     */
    TableResult query(String query, Map<String, QueryParameterValue> namedParameters);

    /**
     * Executes a batch query with the specified SQL string.
     *
     * @param query The SQL query string.
     * @return The result of the batch query.
     */
    TableResult queryBatch(String query);

    /**
     * Executes a batch parameterized query with the specified SQL string and named parameters.
     *
     * @param query           The SQL query string.
     * @param namedParameters Named parameters for the query.
     * @return The result of the batch parameterized query.
     */
    TableResult queryBatch(String query, Map<String, QueryParameterValue> namedParameters);

    /**
     * Executes a parameterized query and applies the specified consumer to each result row.
     *
     * @param query           The SQL query string.
     * @param namedParameters Named parameters for the query.
     * @param forEach         Consumer to apply to each result row.
     */
    void query(String query, Map<String, QueryParameterValue> namedParameters, Consumer<FieldValueListDecorator> forEach);

    /**
     * Executes a query and applies the specified consumer to each result row.
     *
     * @param query   The SQL query string.
     * @param forEach Consumer to apply to each result row.
     */
    void query(String query, Consumer<FieldValueListDecorator> forEach);

    /**
     * Retrieves a single result from a query and applies the specified extractor function.
     *
     * @param query     The SQL query string.
     * @param extractor Extractor function for processing the result.
     * @return The extracted result.
     */
    <T> T get(String query, Function<Iterable<FieldValueList>, T> extractor);

    /**
     * Retrieves a list of results from a query and applies the specified extractor function.
     *
     * @param query     The SQL query string.
     * @param extractor Extractor function for processing each result.
     * @return The list of extracted results.
     */
    <T> List<T> list(String query, Function<FieldValueListDecorator, T> extractor);

    /**
     * Retrieves a list of results from a parameterized query and applies the specified extractor function.
     *
     * @param query           The SQL query string.
     * @param namedParameters Named parameters for the query.
     * @param extractor       Extractor function for processing each result.
     * @return The list of extracted results.
     */
    <T> List<T> list(String query, Map<String, QueryParameterValue> namedParameters, Function<FieldValueListDecorator, T> extractor);

    /**
     * Retrieves and applies the specified consumer to each result row of a table identified by its ID.
     *
     * @param tableId  The ID of the target table.
     * @param pageSize The page size for listing results.
     * @param forEach  Consumer to apply to each result row.
     */
    void list(TableId tableId, int pageSize, Consumer<FieldValueListDecorator> forEach);

    /**
     * Updates records in the specified table using the provided SQL update query.
     *
     * @param query The SQL update query.
     * @return The number of rows affected by the update.
     * @throws InterruptedException If the update process is interrupted.
     */
    long update(String query) throws InterruptedException;

    /**
     * Retrieves and applies the specified consumer to each result row of a table identified by dataset, table name, and schema.
     *
     * @param dataset The name of the dataset containing the table.
     * @param table   The name of the target table.
     * @param schema  The schema of the target table.
     * @param forEach Consumer to apply to each result row.
     */
    void list(String dataset, String table, Schema schema, Consumer<FieldValueListDecorator> forEach);

    /**
     * Retrieves and applies the specified consumer to each result row of a table identified by dataset, table name, schema, and page size.
     *
     * @param dataset  The name of the dataset containing the table.
     * @param table    The name of the target table.
     * @param pageSize The page size for listing results.
     * @param schema   The schema of the target table.
     * @param forEach  Consumer to apply to each result row.
     */
    void list(String dataset, String table, int pageSize, Schema schema, Consumer<FieldValueListDecorator> forEach);

    /**
     * Retrieves a single result from a query and applies the specified extractor function.
     *
     * @param query     The SQL query string.
     * @param extractor Extractor function for processing the result.
     * @return The extracted result.
     */
    <T> T one(String query, Function<FieldValueListDecorator, T> extractor);

    /**
     * Retrieves a single result from a query and applies the specified extractor function.
     * Returns a default value if no result is found.
     *
     * @param query        The SQL query string.
     * @param extractor    Extractor function for processing the result.
     * @param defaultValue The default value to return if no result is found.
     * @return The extracted result or the default value.
     */
    <T> T one(String query, Function<FieldValueListDecorator, T> extractor, T defaultValue);

    /**
     * Creates a table in the specified dataset that is a copy of the source table.
     *
     * @param dataset         The target dataset.
     * @param sourceTableName The source table name.
     * @param targetTableName The target table name.
     */
    void createTableLike(String dataset, String sourceTableName, String targetTableName);

    /**
     * Drops a table in the specified dataset.
     *
     * @param dataset   The dataset containing the table.
     * @param tableName The name of the table to drop.
     * @return True if the table was dropped successfully, false otherwise.
     */
    boolean dropTable(String dataset, String tableName);

    /**
     * Renames a table in the specified dataset.
     *
     * @param dataset The dataset containing the table.
     * @param from    The current table name.
     */
    void renameTable(String dataset, String from, String to);

}
