package com.belkatechnologies.bigquery.configuration;

import com.belkatechnologies.bigquery.configuration.http.HttpTransportOptionsCustom;
import com.belkatechnologies.bigquery.manager.BigQueryManager;
import com.belkatechnologies.bigquery.manager.BigQueryManagerImpl;
import com.belkatechnologies.bigquery.manager.BqDatasetDaoImpl;
import com.belkatechnologies.bigquery.utils.BigQueryMetadataServiceImpl;
import com.belkatechnologies.bigquery.utils.BqQueryUtils;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.threeten.bp.Duration;

/**
 * Auto-configuration class for setting up BigQuery-related beans and services in a Spring Boot application.
 * Configures Google Cloud BigQuery credentials, BigQuery instance, and the BigQueryManager.
 * @author Ilia Guzenko, Denis Chernyshev
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({BigQueryProperties.class})
@Import(value = {BqDatasetDaoImpl.class, BigQueryMetadataServiceImpl.class})
public class BigQueryAutoConfiguration {

    /**
     * Creates a bean for Google Cloud BigQuery credentials based on the provided properties.
     *
     * @param properties The configuration properties for BigQuery.
     * @return Google Cloud BigQuery credentials.
     */
    @Bean
    @ConditionalOnMissingBean
    public GoogleCredentials bqCredentials(BigQueryProperties properties) {
        return BqQueryUtils.getGoogleCredentials(properties.getData().getKeyFile());
    }

    /**
     * Creates a bean for the Google Cloud BigQuery instance with configured settings.
     *
     * @param bqCredentials Google Cloud BigQuery credentials.
     * @return Configured Google Cloud BigQuery instance.
     */
    @Bean
    @ConditionalOnMissingBean
    public BigQuery bigQuery(GoogleCredentials bqCredentials) {
        return BigQueryOptions.newBuilder()
                .setCredentials(bqCredentials)
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(10)
                        .setInitialRetryDelay(Duration.ofSeconds(3))
                        .setMaxRetryDelay(Duration.ofSeconds(10))
                        .setRetryDelayMultiplier(1.5)
                        .setTotalTimeout(Duration.ofMinutes(5))
                        .setInitialRpcTimeout(Duration.ofMillis(50000L))
                        .setRpcTimeoutMultiplier(1.0)
                        .setMaxRpcTimeout(Duration.ofMillis(50000L))
                        .build())
                .setTransportOptions(new HttpTransportOptionsCustom(HttpTransportOptions.newBuilder()
                        .setConnectTimeout(300000)
                        .setReadTimeout(25000)))
                .build()
                .getService();
    }

    /**
     * Creates a bean for the implementation of the BigQueryManager interface.
     *
     * @param bigQuery Configured Google Cloud BigQuery instance.
     * @return Implementation of the BigQueryManager interface.
     */
    @Bean
    @ConditionalOnMissingBean
    public BigQueryManager bigQueryManager(BigQuery bigQuery) {
        return new BigQueryManagerImpl(bigQuery);
    }
}
