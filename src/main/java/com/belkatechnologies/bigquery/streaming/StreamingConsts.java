package com.belkatechnologies.bigquery.streaming;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface StreamingConsts {

    ExecutorService CALLBACK_EXECUTOR = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("append-callback-%d").build());

    int MAX_BYTES = 8_000_000;

    ImmutableList<Status.Code> RETRIABLE_ERROR_CODES =
            ImmutableList.of(
                    Status.Code.INTERNAL,
                    Status.Code.ABORTED,
                    Status.Code.CANCELLED,
                    Status.Code.FAILED_PRECONDITION,
                    Status.Code.DEADLINE_EXCEEDED,
                    Status.Code.UNAVAILABLE);
}
