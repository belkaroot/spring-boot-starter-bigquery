package com.belkatechnologies.bigquery.configuration.http;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;

import java.io.IOException;

public class BigQueryRetryInterceptor implements HttpRequestInitializer {

    private static final int MAX_RETRIES = 10;

    private final HttpRequestInitializer delegate;

    public BigQueryRetryInterceptor(HttpRequestInitializer delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initialize(HttpRequest httpRequest) throws IOException {
        httpRequest.setNumberOfRetries(MAX_RETRIES);
        httpRequest.setIOExceptionHandler(new CustomHttpIOExceptionHandler());
        delegate.initialize(httpRequest);
    }
}
