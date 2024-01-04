# 项目中重要SQL的说明和实现（作者LY）

## 一、一些基础配置

​	以时间字段分区，存储上以\t分隔，null指定为''(空)，每张表存储在hdfs对应的分层目录下。

## 二、ods

​	大部分是常规SQL，需要注意的两点：一是一些表中设置了`old`字段，用来存储旧值，如评论表、收藏表、订单明细表等，结构为`MAP<STRING,STRING>`；二是用`struct`结构存储多值属性，如评论表中data字段包含user_id、user_name等。

## 三、DIM

​	维度数据用orc列式存储，好处是列式存储先存储主键后存储其他字段，查询效率高，减少map tasks，正好契合维度数据经常join的场景。orc可支持多种索引，也可以使用struct、map等数据结构。

​	DIM重点关注拉链表的装载，整体思路是增量数据到来后，先去重保留最新一条数据，再与当前最新的全量表join，组成一个包含新字段和旧字段的大宽表。如果大宽表中新字段不为空就overwrite写入到拉链表最新分区，最后拼接上新旧字段都存在的数据（修改日期）即可。

​	具体实现是先构建大宽表，然后拼接，过程中加入md5加密。

```sql
with
tmp as
(
    select
        old.id old_id,
        ...
        old.end_date old_end_date,
        new.id new_id,
        ...
        new.end_date new_end_date
    from
    (
        select
            id,
            ...
            end_date
        from dim_user_zip
        where dt='9999-12-31'
    )old
    full outer join
    (
        select
            id,
            ...
            md5(name) name,
            md5(phone_num) phone_num,
            md5(email) email,
            ...
            '2023-09-15' start_date,
            '9999-12-31' end_date
        from
        (
            select
                data.id,
               	...
                row_number() over (partition by data.id order by ts desc) rn
            from ods_user_info_inc
            where dt='2023-09-15'
        )t1
        where rn=1
    )new
    on old.id=new.id
)
insert overwrite table dim_user_zip partition(dt)
select
    if(new_id is not null,new_id,old_id),
    ...
    if(new_id is not null,new_end_date,old_end_date) dt
from tmp
union all
select
    old_id,
    ...
    cast(date_add('2023-09-15',-1) as string) old_end_date,
    cast(date_add('2023-09-15',-1) as string) dt
from tmp
where old_id is not null
and new_id is not null;

```

​	日期维度表包含日期、每一天是否节假日等，因为是固定的所以一次写入n年，可以直接导入网上通用的date_info，即创立临时表导入本地文件、再写入正式维度表。

## 四、DWD

​	DWD存储事务表，因为也要被上层join，选用orc列式存储。指定`hive.exec.dynamic.partition.mode=nonstrict`，使所有分区字段都可以使用动态分区。

​	对于事务事实表都是落盘在当日分区，增量数据的提取靠增量表的type字段区分；周期快照事务表如购物车表，从ods的全量表中提取。

## 五、DWS

​	根据ADS的需求，统计周期分为最近一日、最近三十日、历史至今三个周期，对DWD的数据进行聚合计算。这里要选好粒度，最好配合业务线性矩阵去做。

​	对于最近一日的聚合，基本上就是简单聚合；最近n日的聚合通过`date_add`控制日期，对时间段内的最近一日汇总的所有数据再聚合。历史至今的首日聚合装载是计算所有的最近一日数据，每日更新时，将当日最近一日数据与前一天的历史至今数据合并成新的历史至今表。

## 六、ADS

### 1、lateral view explode的使用

​	ADS是离线重点，对各个主题的指标进行统计，要注意统计时类型的转换，比如从int转换成bigint。以及注意在表示统计范围时`lateral view explode`的使用。

```sql
insert overwrite table ads_traffic_stats_by_channel
select * from ads_traffic_stats_by_channel
union
select
    ...
    recent_days,
   ...
from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('2023-09-14',-recent_days+1)
group by recent_days,channel;
```

​	比如上面的sql中拆分数组（1,7,30）作为recent_day字段。

### 2、路径分析

​	项目中的用户行为日志包含了用户访问路径，路径分析的可视化桑基图需要跳转信息。而跳转信息可以通过lead函数取到下一个访问路径，构建一个包含每一步跳跃的表，再统计每一步所有用户的跳跃步数（也就是页面跳转的热度）。

```sql
insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2023-09-14' dt,
    recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
        recent_days,
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
            recent_days,
            page_id,
            lead(page_id,1,null) over(partition by session_id,recent_days) next_page_id,
            row_number() over (partition by session_id,recent_days order by view_time) rn
        from dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
        where dt>=date_add('2023-09-14',-recent_days+1)
    )t1
)t2
group by recent_days,source,target;
```

​	从最里层看起，先利用lead函数按照主键id和统计范围分区后的、时间戳从小到大的顺序取下一跳的位置，并通过row_number函数给每一条标上序号。

​	然后给每一条的起始和终止点按照序号组合，比如一条数据表示将要从page3跳到page4，rn为3，那么组合为step-3:page3，step-4:page4。

​	最外层判断终点，并count聚合计算相同跳跃发生了几次，即计算热度。

### 3、流失用户和回流用户

​	DWS层的用户登录表有最后登录日期的字段，有数据就说明以前活跃过，所以流失用户直接读表聚合统计最后登录在七天前的用户。

​	回流用户通过join昨天登录表和今日登录表，筛选出两个最后登录日期大于n天的数据，再聚合统计即可。下面是计算回流用户的sql：

```sql
select
        '2023-09-14' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from dws_user_user_login_td
        where dt='2023-09-14'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from dws_user_user_login_td
        where dt=date_add('2023-09-14',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
```

### 4、用户留存率

​	包括新增留存和活跃留存两方面。新增留存分析是分析某天的新增用户中，有多少人有后续的活跃行为。活跃留存分析是分析某天的活跃用户中，有多少人有后续的活跃行为。

​	关键是要以新用户为着眼点写sql。

​	先用七天内新用户的注册表join今天的登录表，如果登录表中的最后登录日期是今天，那么他就是留存用户，反之同理。然后用sum统计留存用户个数，count统计新增用户数量，最后相除就是用户留存率。

```sql
insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2023-09-14' dt,
    login_date_first create_date,
    datediff('2023-09-14',login_date_first) retention_day,
    sum(if(login_date_last='2023-09-14',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='2023-09-14',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
(
    select
        user_id,
        date_id login_date_first
    from dwd_user_register_inc
    where dt>=date_add('2023-09-14',-7)
    and dt<'2020-06-14'
)t1
join
(
    select
        user_id,
        login_date_last
    from dws_user_user_login_td
    where dt='2023-09-14'
)t2
on t1.user_id=t2.user_id
group by login_date_first;
```

### 5、用户行为漏斗分析

​	漏斗分析简单来说就是统计从业务链开始的人数变化，具体sql就是分时间范围去统计各个业务的有效人数，最后join。

### 6、TopN

​	直接用rank（）over函数排序。	

```
rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
```

