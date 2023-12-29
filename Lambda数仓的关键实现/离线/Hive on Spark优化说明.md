# Hive on Spark优化说明（作者LY）

## 一、Spark参数优化

​	前置：yarn的nodemanager可分配内存为64G，可分配核数为16。	

​	Executor的cpu核数4-6，项目中单个节点16核，设定为4核可以充分利用资源。参考最佳实践的计算方法：可分配内存 * （Executor核数/nodemanager可分配核数）= 16。然后按照10:1的比例分配堆内存和堆外内存，即14G和2G，最终配置为：

```shell
spark.executor.memory    14G
spark.executor.memoryOverhead    2G
```

​	Executor个数选择动态分配，本项目采用初始值1，最大值12，实际可以调整。要注意开启动态分配的同时要开启Spark Shuffle服务，因为动态分配下空闲Executor关闭后输出文件需要被shuffle管理。还要注意指定积压任务的等待时长，超过1s申请新的Executor。

​	关于Driver的堆内存设置，网上有个帖子写了一种经验论：如果nodemanager可分配内存大于50G则设置堆内存总和为12G，如果在12G到50G之间设置为4G，如果在1G到12G之间设置1G。之后再按照10：1分配堆内堆外。项目64G，因此设置堆内10G，堆外2G。

## 二、分组聚合优化

​	对于项目中的count(),sum()等单调的聚合需求，可以采用map-side聚合优化。本质上和MapReduce编程中的map端预聚合的原理相同，都是在map端维护一个hash table完成分区内的部分聚合，最后将部分聚合发给reduce端，这样就能有效减少shuffle量，只不过hive自带了参数不用手动实现了。	

```hive
--启用map-side聚合
set hive.map.aggr=true;
--hash map占用map端内存的最大比例
set hive.map.aggr.hash.percentmemory=0.5;
--用于检测源表是否适合map-side聚合的条数。
set hive.groupby.mapaggr.checkinterval=100000;
--map-side聚合所用的HashTable，占用map任务堆内存的最大比例，若超出该值，则会对HashTable进行一次flush。
set hive.map.aggr.hash.force.flush.memory.threshold=0.9;
```

## 三、join优化

​	hive的join有三种：

​	第一种common join，map端读取表数据，按照关联字段分区，然后交给reduce端关联，这种就是默认join。 

​	第二种map join，map端缓存小表的所有数据，然后扫描另一张大表进行关联。这种优化适用于一大一小表，在我的项目中诸如dwd层与维度表join，与省份表join等都用到了这种优化。

```hive
--启用map join自动转换
set hive.auto.convert.join=true;
--common join转map join小表阈值,这个参数需要自己调整
set hive.auto.convert.join.noconditionaltask.size = xxx;
```

​	第三种Sort Merge Bucket Map Join，使用条件是参与join的表均为分桶表，且关联字段为分桶字段，且分桶字段是有序的，且大表的分桶数量是小表分桶数量的整数倍。

​	在我的项目里使用主键分桶的情况多，并且分桶数量一般是2的n次冥。具体使用方式有两种，第一种是预估数据的规模，在建表的时候就使用分桶表；另一种是当join效率过低时，符合条件的表转储到分桶表后再join。建表时关键语句如下，指定主键和排序字段以及桶数量：

```hive
clustered by (user_id) sorted by(user_id) into 32 buckets
```

​	桶的大小没有要求，最后设置参数、调整SQL语句就可以了。

```hive
--启动Sort Merge Bucket Map Join优化
set hive.optimize.bucketmapjoin.sortedmerge=true;
--使用自动转换SMB Join
set hive.auto.convert.sortmerge.join=true;
```

## 四、数据倾斜优化

​	数据倾斜指计算过程中某一种key对应的数据过大，数据分布不均，进而在shuffle的时候大量相同key的数据发给一个reduce造成阻塞。对于数据倾斜的优化分两方面：

​	如果是分组聚合导致的数据倾斜，如我项目中统计交易详情表中按省份统计总数，某一省份的交易和其他省份的交易数不在一个数量级，那么可以开启map-side聚合环节reduce端的压力，同时开启skew groupby在外层将数据分散、最终在聚合。	

```hive
set hive.map.aggr=true;
set hive.map.aggr.hash.percentmemory=0.5;
--启用分组聚合数据倾斜优化
set hive.groupby.skewindata=true;
```

​	如果是join导致的数据倾斜，比如某一省份和交易详情表join，省份key对应的数据量过大，除了map join外，如果目标是inner join，还可以启用skew join。skew join原理的本质就是如果某个key发生了倾斜，就先保存在hdfs中，同时开启另外一个job单独用map join处理，最终倾斜数据的结果和非倾斜数据的结果合并。

```hive
--启用skew join优化
set hive.optimize.skewjoin=true;
--触发skew join的阈值，若某个key的行数超过该参数值，则触发
set hive.skewjoin.key=100000;
```

​	也可以在建表时就指定skew join的倾斜字段。

## 五、其他优化

​	Map并行度无调整，Reduce并行度Hive可以自行估算，但是想要让这个估算准确，需要让Hive收集到足够的信息，因此配置以下参数：

```hive
--执行DML语句时，收集表级别的统计信息
set hive.stats.autogather=true;
--执行DML语句时，收集字段级别的统计信息
set hive.stats.column.autogather=true;
--计算Reduce并行度时，从上游Operator统计信息获得输入数据量
set hive.spark.use.op.stats=true;
--计算Reduce并行度时，使用列级别的统计信息估算输入数据量
set hive.stats.fetch.column.stats=true;
```

​	开启小文件合并，map端防止小文件浪费资源，reduce端减少hdfs小文件数量。

```hive
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 
set hive.merge.sparkfiles=true;
```

