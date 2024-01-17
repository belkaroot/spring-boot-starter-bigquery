package com.belkatechnologies.bigquery.utils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.QueryJobConfiguration;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.util.Objects;

/**
 * Utility class for Google BigQuery operations and Google Cloud credentials.
 * @author Ilia Guzenko, Denis Chernyshev
 */
public class BqQueryUtils {

    /**
     * Creates a BigQuery query job configuration based on the provided SQL query.
     *
     * @param query The SQL query.
     * @return Query job configuration for the given query.
     */
    public static QueryJobConfiguration createQuery(final String query) {
        return QueryJobConfiguration.newBuilder(query).build();
    }

    /**
     * Retrieves Google Cloud credentials based on the provided key file path.
     * If the key file path is null, it attempts to retrieve application default credentials.
     *
     * @param keyFile The path to the Google Cloud key file.
     * @return Google Cloud credentials.
     * @throws RuntimeException If there is an issue loading the credentials.
     */
    public static GoogleCredentials getGoogleCredentials(String keyFile) {
        if (keyFile != null) {
            try {
                return GoogleCredentials.fromStream(
                        Objects.requireNonNull(BqQueryUtils.class.getClassLoader().getResourceAsStream(keyFile)));
            } catch (IOException e) {
                throw new RuntimeException("Failed while loading BqKeyFile, please check if it is correct!", e);
            }
        }
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new RuntimeException("Failed while loading GOOGLE_APPLICATION_CREDENTIALS!", e);
        }
    }
}

