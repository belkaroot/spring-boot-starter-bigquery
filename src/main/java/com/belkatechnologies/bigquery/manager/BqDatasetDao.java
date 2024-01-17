package com.belkatechnologies.bigquery.manager;

/**
 * Interface defining operations for managing Google Cloud BigQuery datasets.
 */
public interface BqDatasetDao {

    /**
     * Checks if a dataset with the specified name exists.
     *
     * @param datasetName The name of the dataset.
     * @return True if the dataset exists, false otherwise.
     */
    boolean exists(String datasetName);

    /**
     * Creates a new dataset with the specified name.
     *
     * @param datasetName The name of the dataset to create.
     * @return True if the dataset is created successfully, false otherwise.
     */
    boolean create(String datasetName);
}

