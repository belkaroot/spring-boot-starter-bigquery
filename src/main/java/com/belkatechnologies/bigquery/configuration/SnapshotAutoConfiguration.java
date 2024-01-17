package com.belkatechnologies.bigquery.configuration;

import com.belkatechnologies.bigquery.snapshotting.BqSnapshotDaoImpl;
import com.belkatechnologies.bigquery.snapshotting.SnapshotServiceImpl;
import com.belkatechnologies.bigquery.snapshotting.SnapshotStatisticsImpl;
import com.belkatechnologies.bigquery.snapshotting.StreamBufferCheckerImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author Ilia Guzenko
 */
@Slf4j
@Configuration
@Import(value = {
        StreamBufferCheckerImpl.class,
        BqSnapshotDaoImpl.class,
        SnapshotStatisticsImpl.class,
        SnapshotServiceImpl.class
})
//@ConditionalOnProperty(value = "bigquery.snapshotting.enabled", havingValue = "true")
public class SnapshotAutoConfiguration {

}
