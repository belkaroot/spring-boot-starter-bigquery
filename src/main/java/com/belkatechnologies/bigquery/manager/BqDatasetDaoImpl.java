package com.belkatechnologies.bigquery.manager;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BqDatasetDaoImpl implements BqDatasetDao{

    private final BigQueryManager bq;

    @Override
    public boolean exists(String datasetName) {
        Dataset dataset;
        try {
            dataset = bq.getBigQuery().getDataset(DatasetId.of(datasetName));
        } catch (BigQueryException e) {
            log.error("Error dataset exist check", e);
            return false;
        }
        return dataset != null;
    }

    @Override
    public boolean create(String datasetName) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName)
                .setLocation("eu")
                .build();
        Dataset newDataset;
        try {
            newDataset = bq.getBigQuery().create(datasetInfo);
        } catch (BigQueryException e) {
            log.error("Error creating dataset " + datasetName, e);
            return false;
        }
        String newDatasetName = newDataset.getDatasetId().getDataset();
        log.info(newDatasetName + " created successfully");
        return true;
    }
}
