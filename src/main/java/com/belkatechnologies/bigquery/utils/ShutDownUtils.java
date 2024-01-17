package com.belkatechnologies.bigquery.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ShutDownUtils {

    public static void shutdownWithAwait(ExecutorService executorService, int timeout, TimeUnit timeUnit, String name) {
        log.debug("Shutdown executor {}", name);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(timeout, timeUnit)) {
                log.warn("Can't terminate executor {}, trying force", name);
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Error terminate executor {}, trying force", name, e);
            executorService.shutdownNow();
        }
    }

}
