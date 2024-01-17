package com.belkatechnologies.bigquery.snapshotting;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A record representing configuration for creating snapshots.
 * <p>
 * This record includes parameters such as expiration days, log table dataset, wait buffer, and datasets.
 * It also provides a builder for convenient instantiation.
 * </p>
 *
 * @param expirationDays The number of days until the snapshot expires.
 * @param waitBuffer     A boolean flag indicating whether to wait for a buffer before creating snapshots.
 * @param datasets       A mapping of datasets to the corresponding sets of tables to be included in snapshots.
 */
public record SnapshotConfiguration(int expirationDays, boolean waitBuffer,
                                    Map<String, Set<String>> datasets) {

    /**
     * Creates a new instance of {@link SnapshotConfigurationBuilder}.
     *
     * @return A new instance of the builder for {@link SnapshotConfiguration}.
     */
    public static SnapshotConfiguration.SnapshotConfigurationBuilder builder() {
        return new SnapshotConfiguration.SnapshotConfigurationBuilder();
    }

    /**
     * Builder class for {@link SnapshotConfiguration}.
     */
    public static class SnapshotConfigurationBuilder {
        private int expirationDays;
        private boolean waitBuffer;
        private final Map<String, Set<String>> datasets = new HashMap<>();

        /**
         * Sets the expiration days for the snapshot.
         *
         * @param expirationDays The number of days until the snapshot expires.
         * @return The current builder instance for method chaining.
         */
        public SnapshotConfigurationBuilder expirationDays(int expirationDays) {
            this.expirationDays = expirationDays;
            return this;
        }

        /**
         * Sets the wait buffer flag for the snapshot.
         *
         * @param waitBuffer A boolean flag indicating whether to wait for a buffer before creating snapshots.
         * @return The current builder instance for method chaining.
         */
        public SnapshotConfigurationBuilder waitBuffer(boolean waitBuffer) {
            this.waitBuffer = waitBuffer;
            return this;
        }

        /**
         * Adds a dataset and its corresponding set of tables to be included in snapshots.
         * if set is empty then all tables will be added to snapshot candidates
         *
         * @param dataset The dataset to be added.
         * @param tables  The set of tables associated with the dataset.
         * @return The current builder instance for method chaining.
         */
        public SnapshotConfigurationBuilder add(String dataset, Set<String> tables) {
            this.datasets.put(dataset, tables);
            return this;
        }

        /**
         * Adds multiple datasets and their corresponding sets of tables to be included in snapshots
         * if set is empty then all tables will be added to snapshot candidates.
         *
         * @param datasets A mapping of datasets to sets of tables.
         * @return The current builder instance for method chaining.
         */
        public SnapshotConfigurationBuilder addAll(Map<String, Set<String>> datasets) {
            this.datasets.putAll(datasets);
            return this;
        }

        /**
         * Builds an instance of {@link SnapshotConfiguration} using the configured parameters.
         *
         * @return An instance of {@link SnapshotConfiguration} with the specified configuration.
         */
        public SnapshotConfiguration build() {
            return new SnapshotConfiguration(expirationDays, waitBuffer, datasets);
        }
    }
}

