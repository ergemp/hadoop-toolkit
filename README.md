# hadoop_toolkit

Hadoop Toolkit supports three features for the time being. 

1- retention manager
2- file merger
3- json value counter 

# Retention Manager 
Applies the given retention parameters to the specified directory and/or with subdirectories.
hadoop_toolkit.hdfs_retention_mgr package uses the following input parameters
```
-hdfsConnect
  HDFS connection string 
  ex: hdfs://127.0.0.1:8020
  
-hdfsPath
  PATH of the files to apply retention
  ex: /logs/postgres/dev01
  
-retentionVal
  VALUE of the retention metric, if retention metric is DAY then this much of days will be applied to retention
  ex: INTEGER_VLAUE
  
-retentionMetric
  METRIC of the retentionVal value. 
  ex: DAY | HOUR | MINUTE
  
-action
  The default action is LIST if not specified
  LIST | DELETE
```
# Example 1 - Listing files older than 10 Hours
```
java -cp hadoop_toolkit-201909-jar-with-dependencies.jar org.ergemp.toolkit.hadoop.processors.hdfs.RetentionManager \
-hdfsConnect "hdfs://heCluster01:8020" \
-hdfsPath "/stardust/postgres_test_streaming_logs/testdb01" \
-retentionVal 10 \
-retentionMetric HOUR
```
# Example 2 - Deleting files older than 10 Days
```
java -cp hadoop_toolkit-201909-jar-with-dependencies.jar org.ergemp.toolkit.hadoop.processors.hdfs.RetentionManager \
-hdfsConnect "hdfs://127.0.0.1:9000" \
-hdfsPath "/stardust/postgres_test_streaming_logs/testdb01" \
-retentionVal 10 \
-retentionMetric DAY \
-action DELETE 
```

# File Merger
Merges the files within a folder with the supplied parameters.
hadoop_toolkit.hdfs_file_merger package uses the following input parameters 
```
-hdfsConnect
  HDFS connection string 
  ex: hdfs://127.0.0.1:8020
  
-hdfsPath
  PATH of the files to apply retention
  ex: /logs/postgres/dev01
  
-sizeLimit
  The SIZE of the files which are below this LIMIT will be merged.
  ex: <INTEGER>
  
-fileLimit
  The number of files which are going to be merged. 
  ex: <INTEGER>

-compress
  If the merged file be compressed. Default is FALSE.
  TRUE | FALSE
```
# Example 3 - Merging Files which are smaller than 100M in size
```
java -cp hadoop_toolkit-201909-jar-with-dependencies.jar org.ergemp.toolkit.hadoop.processors.hdfs.FileMerger \
-hdfsConnect "hdfs://127.0.0.1:8020" \
-hdfsPath "/stardust/postgres_test_streaming_logs/testdb01" \
-sizeLimit 100
-fileLimit 500
-compress true
```
# JSON value counter
Counts the value of a JSON key and writes the output to a defined HDFS path. The optional Filter parameters takes an regex expression and filters the values of the JSON line
USAGE:
  Usage: %s [generic options] <input> <output> <jsonKey> <jsonFilter(optional)>
# Example 4 - Counting the values of "pid" key
```
java -cp dist/hadoop_toolkit.jar hadoop_toolkit.json_labelCounter \
-fs "hdfs://localhost:8020/" \
-jt "localhost:8032" \
"/mockdata/clickStream.json" \
"/outdata/labelCounter" \
"pid"
```
# Example 5 - Counting the values of "event" key after filtering the not null userIds
```
java -cp dist/hadoop_toolkit.jar hadoop_toolkit.json_labelCounter \
  -fs "hdfs://localhost:8020/" \
  -jt "localhost:8032" \
  "/mockdata/clickStream.json"  \
  "/outdata/labelCounter" \
  "event" \
  '^((?!userId.:null).)*$'
```
# Example 6 - Counting the values of "event" key after filtering the null userIds  
```
java -cp dist/hadoop_toolkit.jar hadoop_toolkit.json_labelCounter \
-fs "hdfs://localhost:8020/" \
-jt "localhost:8032" \
"/mockdata/clickStream.json"  \
"/outdata/labelCounter" \
"event" \
'.*userId.:null.*'
```
# Example 7 - Counting the values of "event" by filtering nothing
```
java -cp dist/hadoop_toolkit.jar hadoop_toolkit.json_labelCounter \
-fs "hdfs://localhost:8020/" \
-jt "localhost:8032" \
"/mockdata/clickStream.json" \
"/outdata/labelCounter" \
"event" \
'.*'  
```
  
# Note

If you are using High Available Namenodes then you need to include core-site.xml and hdfs-site.xml configurations to your classpath in order to resolve the clustername.

```
java -cp "hadoop_toolkit-201909-jar-with-dependencies.jar:$HADOOP_HOME/etc/hadoop" org.ergemp.toolkit.hadoop.processors.hdfs.RetentionManager \
-hdfsConnect "hdfs://heCluster01:8020" \
-hdfsPath "/kafka/realty-changed/partdate=2019092300" \
-retentionVal 10 \
-retentionMetric HOUR
```


