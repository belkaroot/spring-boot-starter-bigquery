package com.belkatechnologies.bigquery.configuration.http;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;

public class HttpTransportOptionsCustom extends HttpTransportOptions {

    public HttpTransportOptionsCustom(Builder builder) {
        super(builder);
    }

    @Override
    public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions) {
        return new BigQueryRetryInterceptor(super.getHttpRequestInitializer(serviceOptions));
    }
}