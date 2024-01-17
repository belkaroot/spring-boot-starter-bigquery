package com.belkatechnologies.bigquery.configuration;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Ilia Guzenko, Denis Chernyshev
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "bigquery", ignoreUnknownFields = true)
public class BigQueryProperties {

    private DataProperties data;
    private StreamingProperties streaming;
    private SnapshottingProperties snapshotting;

    @Getter
    @Setter
    public static class DataProperties {
        private String project;
        private String keyFile;
    }

    @Getter
    @Setter
    public static class StreamingProperties {
        private boolean enabled;
        private Integer streamingManagerPoolSize;
        private Integer asyncStreamingDelay;
    }

    @Setter
    @Getter
    public static class SnapshottingProperties {
        private boolean enabled;
        private String logTableDataset;
    }
}
