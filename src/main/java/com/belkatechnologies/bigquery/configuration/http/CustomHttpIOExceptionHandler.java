package com.belkatechnologies.bigquery.configuration.http;

import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CustomHttpIOExceptionHandler implements HttpIOExceptionHandler {

    private final BackOff backOff = new ExponentialBackOff();

    @Override
    public boolean handleIOException(HttpRequest request, boolean supportsRetry) throws IOException {
        log.error("Got exception while http request, supportsRetry={}, retry attempt={}", supportsRetry, request.getNumberOfRetries());
        if (!supportsRetry) {
            return false;
        }
        try {
            return BackOffUtils.next(Sleeper.DEFAULT, backOff);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
