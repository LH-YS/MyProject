# 入湖FlinkSQL配置的个人笔记（作者LY）

## 一、预处理时需要注意的配置

​	简单来说项目是把三张表从kafka中入湖到hudi中，hudi与hive互通，利用hive去调度三张表。入湖的流程为：

​	kafka源日志—提取需要的字段到ods表—去重处理到dwd表—最后从kafka入湖到hudi/hive

​	其中在入湖FlinkSQL需要的配置：

​	ods处理的相关配置（从kafka的源日志中提取需要的字段）如下，`scan.startup.mode`设置为`latest-offset`，如果重启任务可以设置为`group-offsets`。

```sql
		WITH (
          	'connector' = 'kafka',
          	'properties.bootstrap.servers' = 'hadoop1:9092',
            'properties.group.id' = 'dwd_creator_penalize_ticket',
          	'kafka.topic' ='ods_creator_penalize_ticket',
          	'format' = 'json',
          	-- 'scan.startup.mode' = 'latest-offset',
          	'scan.startup.mode' = 'timestamp',
            'scan.startup.timestamp-millis' = '160000000000',
            'parallelism' = '5'
        );
```

​	ods的结果并不是直接在dwd表去重，而且建立一个视图，预防性的去重。按照主键和更新时间排序，只取最新一条。

```sql
CREATE  VIEW base_view AS
SELECT  *
FROM    (
            SELECT  *, -- 去重和排序(同一条数据只保留最新的数据)
                    ROW_NUMBER() OVER (
                        PARTITION BY
                                penalize_id
                        ORDER BY
                                binlog_file_name DESC,
                                binlog_relative_offset DESC
                    ) AS row_num
            FROM    ods_creator_penalize_ticket
        ) a
WHERE   a.row_num = 1;
```

​	去重后通过视图写入dwd，和ods一样以JSON格式储存在kafka的topic中，需要注意的是要预留字段，设置`extra`字段和 `json_parser(extra, 'biz_trace_id') AS biz_trace_id`字段。



## 二、入湖时需要注意的配置

​	入湖的常用参数（个人用）：

```sql
  'connector' = 'hudi',
  'path' = 'hdfs://path/',
  'index.type' = 'BUCKET',                 
  'hoodie.parquet.compression.codec'= 'snappy',
  'table.type' = 'COPY_ON_WRITE',
  'write.operation' = 'upsert', 
  'write.task.max.size' = '2048', 
  'write.precombine' = 'true',
  'write.precombine.field' = 'update_time',
  'write.tasks' = '6',
  'write.bucket_assign.tasks' = '6',
  'hoodie.bucket.index.hash.field' = 'id',    
  'hoodie.bucket.index.num.buckets' = '256',  
  'hive_sync.enable'='true',
  'hive_sync.table'='TABLE_NAME',
  'hive_sync.db'='DB_NAME',
  'hive_sync.mode' = 'hms',
  'hive_sync.metastore.uris' = 'thrift://HOST:9083',
  'hive_sync.skip_ro_suffix' = 'true',
  'write.insert.cluster' = 'true',
  'write.ignore.failed' = 'true',
  'clean.async.enabled' = 'true',
  'clean.retain_commits' = '3', 
  'hoodie.cleaner.commits.retained' = '3',
  'hoodie.keep.min.commits' = '4', 
  'hoodie.keep.max.commits' = '8'
```

​	注意`write.tasks`设置成source的并行度的整数倍，而在我的项目中除了配置以上部分参数外，还配置了FlinkSQL独有的precombie field预聚合。预聚合的时候如果数据重复，与合并字段值最大的那条记录，如果最大值相同取第一个。同样写数据的时候比较ts，保留最新。

```sql
  'write.precombine' = 'true',
  'write.precombine.field' = 'update_time'
```

​	因为要调度的三张表有相同字段的主键，所以在配置hudi表时需要选择bucket数量为2的n次幂，大小128~512mb（因为建立后就不支持改变bucket，所以需要预估数据量），通过参数指定主键和索引，例如：

```sql
  'hoodie.bucket.index.num.buckets' = '1024'
  'hoodie.datasource.write.recordkey.field' = 'penalize_id'
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.hash.field' = 'penalize_id'
```



​	入湖时最重要的两部分参数，一部分是compaction的配置参数，一部分是flinkCP和运行配置。

​	hudi compaction的配置如下，这部分的参数可以直接参考官网或者一些最佳实践的配置：

```sql
  'write.tasks' = 20,
  'compaction.async.enabled' = 'true',
  'compaction.tasks' = '1',
  'compaction.schedule.enabled' = 'true',
  'compaction.trigger.strategy' = 'num_commits',
  'compaction.delta_commits' = '2',
```

​	CP上因为项目是审核机制，对比主营业务一般来说用户违规数量偏少，可以设置ttl为12h或24h，并且因为允许一定程度的延迟，可以开启`mini-batch`增大吞吐量。而这个场景下单个主键、或者说单个用户申请的违规审核次数有前端限制，不会出现过分的数据倾斜，也没有SUM、COUNT等运算聚合，因为不考虑`Local-Global Aggregation`和`Split Distinct Aggregation`这两种常见的FlinkSQL优化参数。

​	综上，可配置为：

```sql
  "state.backend": "rocksdb",
  "table.exec.state.ttl": "24 h",
  "state.backend.incremental": "true",
  "execution.checkpointing.enable": "true",
  "execution.checkpointing.timeout": "20 min",
  "execution.checkpointing.interval": "5 min",
  "taskmanager.memory.managed.fraction": "0.1",
  -- minibatch优化参数
  "table.exec.mini-batch.enabled": "TRUE",
  "table.exec.mini-batch.size": "500",            --单个subtask缓存数据量
  "table.exec.mini-batch.allow-latency": "1000 ms" -- 缓存时长
```

