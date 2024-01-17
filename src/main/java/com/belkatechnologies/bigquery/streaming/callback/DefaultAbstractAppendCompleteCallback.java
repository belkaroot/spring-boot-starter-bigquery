package com.belkatechnologies.bigquery.streaming.callback;

import com.belkatechnologies.bigquery.streaming.processor.BigQueryStreamProcessor;
import com.belkatechnologies.bigquery.streaming.processor.StreamingObject;
import com.google.api.core.ApiFutureCallback;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

import static com.belkatechnologies.bigquery.streaming.StreamingConsts.RETRIABLE_ERROR_CODES;

/**
 * Abstract callback class for handling completion events of BigQuery streaming append requests.
 * Extend this class, annotate the subclass with `@Component` and `@Scope(BeanDefinition.SCOPE_PROTOTYPE)`,
 * and override the necessary methods.
 * <p>
 * The subclass must be annotated with:
 * - `@Component`
 * - `@Scope(BeanDefinition.SCOPE_PROTOTYPE)`
 * <p>
 * The constructor remains unchanged in the subclass.
 * <p>
 * Note: The methods `doOnSuccess`, `doOnSuccessButHasError`, and `doOnFailure` must be overridden
 * in the subclass to provide custom logic for success and failure scenarios.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class DefaultAbstractAppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    protected final BigQueryStreamProcessor parent;
    protected final StreamingObject batch;
    private final Phaser phaser;
    protected final AtomicLong processedRows;
    protected final AtomicLong processedBytes;

    /**
     * Custom logic to be executed on successful completion of an append operation.
     *
     * @param response The {@link AppendRowsResponse} received upon success.
     */
    public abstract void doOnSuccess(AppendRowsResponse response);

    /**
     * Custom logic to be executed on successful completion of an append operation
     * when an error is detected in the response.
     *
     * @param response The {@link AppendRowsResponse} received upon success.
     */
    public abstract void doOnSuccessButHasError(AppendRowsResponse response);

    /**
     * Custom logic to be executed on failure of an append operation.
     *
     * @param throwable The {@link Throwable} representing the failure.
     */
    public abstract void doOnFailure(Throwable throwable);

    /**
     * Handles the successful completion of an append operation.
     *
     * @param response The {@link AppendRowsResponse} received upon success.
     */
    public final void onSuccess(AppendRowsResponse response) {
        if (response.hasError()) {
            phaser.arrive();
            log.info("table: {}. Error in response {}", parent.getTable().getTable(), response.getError());
            doOnSuccessButHasError(response);
            return;
        }
        log.info("onSuccess for table {}", parent.getTable().getTable());
        processedRows.addAndGet(batch.jsonBatch().length());
        processedBytes.addAndGet(batch.size());
        phaser.arrive();
        doOnSuccess(response);
    }

    /**
     * Handles the failure of an append operation.
     *
     * @param throwable The {@link Throwable} representing the failure.
     */
    public final void onFailure(Throwable throwable) {
        phaser.arrive();
        doOnFailure(throwable);
        String table = parent.getTable().getTable();
        log.info("onFailure for table " + table);
        if (throwable instanceof StatusRuntimeException ex) {
            Status status = Status.fromThrowable(throwable);
            log.error("StatusRuntimeException while processing table {}, status {}", table, ex.getStatus());
            if (Status.INVALID_ARGUMENT.getCode().equals(status.getCode())) {
                log.error("Invalid argument in batch", ex);
                if (ex instanceof Exceptions.AppendSerializtionError) {
                    String error = ((Exceptions.AppendSerializtionError) ex).getRowIndexToErrorMessage().toString();
                    log.error("Can't save batch: {}", error);
                }
            } else if (RETRIABLE_ERROR_CODES.contains(status.getCode())) {
                try {
                    parent.retryBatch(batch);
                } catch (Exception e) {
                    log.error("Exception while retryBatch for table " + table, e);
                }
            }
        } else {
            log.error("Exception while processing table " + table, throwable);
            try {
                parent.retryBatch(batch);
            } catch (Exception e) {
                log.error("Exception while retryBatch for table " + table, e);
            }
        }
    }
}

