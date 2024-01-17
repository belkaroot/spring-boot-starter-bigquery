<h1>Prerequisites</h1>
Before you begin, make sure you have GCP account, a project set up on GCP and the BigQuery API enabled. 
Also make sure you are using SpringBoot 3 and java 17.
Getting started with data streaming
Add maven dependency



```
<dependency>
    <groupId>com.belkatechnologies</groupId>
    <artifactId>big-query-starter</artifactId>
    <version>0.0.1</version>
</dependency>
```

Configure your application.yaml

```
bigquery:
    data:
        #replace my-project with your actual Google Cloud Platform (GCP) project ID where BigQuery resources are located
        project: my-project
        #may be omitted if GOOGLE_APPLICATION_CREDENTIALS env specified
        keyFile: path/to/my-project.json
    streaming:
        #Flag indicating whether streaming is enabled.
        enabled: true 
```


So and in a few lines:
```
@Autowired
private final StreamingManager streamingManager;
//name of table to stream
TableName tableName = TableName.of(bigQueryProperties.getData().getProject(), "examples_dataset", "user_notes_table");
//here row is your record read from Kafka/file/even-queue/etc..
public void streamSomeData(Map<String, Object> row) {
    streamingManager.putRowForTable(tableName, row);
}
```
That's all, without delving into the nuances, 
your record (or multiple records if you use the putBatchForTable method) 
is already in the table, streamed by managed processor.


more documented examples - https://github.com/belkaroot/spring-boot-starter-bigquery-examples
article - TBD