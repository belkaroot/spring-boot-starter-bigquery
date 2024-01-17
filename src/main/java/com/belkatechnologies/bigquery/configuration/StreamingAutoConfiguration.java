package com.belkatechnologies.bigquery.configuration;

import com.belkatechnologies.bigquery.streaming.DefaultStreamingManager;
import com.belkatechnologies.bigquery.streaming.StreamingManager;
import com.belkatechnologies.bigquery.streaming.callback.DefaultAbstractAppendCompleteCallback;
import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.belkatechnologies.bigquery.streaming.processor.DefaultAsyncContinuousRetriableStreamProcessor;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.io.IOException;

/**
 * Auto-configuration class for setting up BigQuery Streaming-related beans and services
 * in a Spring Boot application.
 * Configures components for streaming data into BigQuery.
 * @author Ilia Guzenko
 */
@Configuration
@ConditionalOnProperty(value = "bigquery.streaming.enabled", havingValue = "true")
public class StreamingAutoConfiguration {

    /**
     * Creates BigQuery Write settings based on the provided Google Cloud credentials.
     *
     * @param bqCredentials Google Cloud credentials.
     * @return BigQuery Write settings.
     * @throws IOException If there is an issue with the provided credentials.
     */
    @Bean
    @ConditionalOnMissingBean
    public BigQueryWriteSettings bigQueryWriteSettings(GoogleCredentials bqCredentials) throws IOException {
        return BigQueryWriteSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(bqCredentials)).build();
    }

    /**
     * Creates a prototype-scoped BigQuery Write client based on the provided settings.
     *
     * @param bigQueryWriteSettings BigQuery Write settings.
     * @return BigQuery Write client.
     * @throws IOException If there is an issue with the provided settings.
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BigQueryWriteClient bigQueryWriteClient(BigQueryWriteSettings bigQueryWriteSettings) throws IOException {
        return BigQueryWriteClient.create(bigQueryWriteSettings);
    }

    /**
     * Creates a prototype-scoped instance of the default asynchronous, continuous, and retriable
     * BigQuery Stream Processor, based on the provided BigQuery Write client and callback provider.
     *
     * @param bigQueryWriteClient  BigQuery Write client.
     * @param callbackProvider     Provider for the abstract append-complete callback.
     * @return Default asynchronous, continuous, and retriable BigQuery Stream Processor.
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @ConditionalOnMissingBean
    public BigQueryStreamProcessor defaultAsyncRetriableStreamProcessor(
            BigQueryWriteClient bigQueryWriteClient,
            ObjectProvider<DefaultAbstractAppendCompleteCallback> callbackProvider
    ) {
        return new DefaultAsyncContinuousRetriableStreamProcessor(bigQueryWriteClient, callbackProvider);
    }

    /**
     * Creates a Streaming Manager based on the provided BigQuery properties and
     * BigQuery Stream Processor object factory.
     *
     * @param bigQueryProperties                 BigQuery configuration properties.
     * @param bigQueryStreamProcessorObjectFactory Object factory for creating BigQuery Stream Processor instances.
     * @return Default Streaming Manager.
     */
    @Bean
    @ConditionalOnMissingBean
    public StreamingManager streamingManager(
            BigQueryProperties bigQueryProperties,
            ObjectFactory<BigQueryStreamProcessor> bigQueryStreamProcessorObjectFactory
    ) {
        return new DefaultStreamingManager(bigQueryProperties, bigQueryStreamProcessorObjectFactory);
    }
}

