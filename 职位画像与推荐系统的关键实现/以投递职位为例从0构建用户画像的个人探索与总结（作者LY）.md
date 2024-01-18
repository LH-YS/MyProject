# 以投递职位为例从0构建用户画像的个人探索与总结（作者LY）

## 一、简述

​	数据来源不易，以前做过MapReduce实现的协同过滤音乐推荐系统，当时使用的是百万音乐数据集（The Million Song Dataset ），其中歌曲名、用户名等敏感信息经过了md5加密处理，实际操作下来有很多不便之处，如文本处理，结果显示等都需要转化。

​	本次我找到了一份有关职位投递的轻量级未加密的数据，源表为职位表、公司表、用户信息三张基本信息表和用户投递行为表，总共四张表组成。其中职位表约28w条数据，用户投递表约250w条数据，因此实践中计算的量级在百万级至千万级左右（因为硬件限制在计算相似度与召回推荐的部分我会控制数据量大小进而控制shuffle的磁盘读写io）。

​	我对用户画像的理解是：用户画像是体现用户信息与行为的背后意义的一个直观地展示。那么最后呈现的效果应该包括用户基本信息、用户标签以及对应的权重。在我的数据中，投递信息是用户id、投递岗位、时间组成的三元组，职位信息中包含职位id、名称、地区、具体描述等。在阅读过用户画像的相关书籍后，我选择文本分析的方式去构建用户画像和职位画像，基本思路是**先组合文本再构建职位画像，最后基于职位画像和投递信息进一步构建用户画像**。

​	接下来会具体介绍构建细节。

## 二、数据流向

​	简单的ETL流程：源表来自MySql，通过datax全量导入hbase，hive建立对应的映射表。增量数据通过maxwell导入kafka再经flink处理put到hbase。OLAP选择Clickhouse对接BI（暂未实现）。投递信息设计上也由flink处理，具体为拆解json再封装成三元组。

​	最后要处理的数据就是hive可映射的三张表，分别命名为position、company、account。结构如下：

## 三、组合文本

​	在文本分析前先组合**包含足够信息的文本**。在职位表中可得到的信息有：职位名、所在地区、行业、职位描述、所属公司等等，其中所属公司本身就具有专属性（相当于自带的聚类属性）如果进行计算会破坏原有的分类，故不用加入组合文本。

​	综上，选定职位名、所在地区、行业、职位描述等信息进行组合。基于pyspark执行sql：

```python
oa.spark.sql("use ods")
    position_content= oa.spark.sql("select id,region,positionsecondcategory,"
                                   "concat_ws('_',positionname,region,desc) as content "
                                   "from ods_position1 ")
    position_content.registerTempTable("tmpcontent")
    oa.spark.sql("insert overwrite table ods_position_content select * from tmpcontent")
```

## 四、文本分析

### 1、分词

​	文本分析就是提取组合文本的关键词、主题词，并在此基础上计算词与词的关系和和权重。

​	首先对职位的组合文本分词，分词函数如下：

```python
#分词函数    
def segmentation(partition):
    import os
    import re
    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs
    #加载词典目录，词典目录下存放用户词典和关键词词典
    abspath = "../../data/words"
    #jieba加载用户词典
    userDict_path = os.path.join(abspath,"ITKeywords.txt")
    jieba.load_userdict(userDict_path)
    #停用词路径
    stopwords_path = os.path.join(abspath,"stopwords.txt")
    
    def get_stopwords_list():
        """返回停用词列表"""
        stopwords_list = [i.strip() for i in codecs.open(stopwords_path,encoding="utf-8").readlines()]
        return stopwords_list
    #获取停用词列表
    stopwords_list = get_stopwords_list()
    
    #分词，函数传入一条文本
    def cut_sentence(sentence):
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        seg_list = pseg.lcut(sentence)
        #去掉停用词
        seg_list = [i for i in seg_list if i.flag not in stopwords_list]
        filtered_words_list = []
        for seg in seg_list:
            if len(seg.word)<=1:
                #单个字或空不收集
                continue
            elif seg.flag == "eng":
                #英文保留长度>2
                if len(seg.word) <= 2:
                    continue
                else:
                    filtered_words_list.append(seg.word)
            elif seg.flag.startswith("n"):
                #保留名词
                filtered_words_list.append(seg.word)
            elif seg.flag in ["x","eng"]: # 是自定一个词语或者是英文单词
                filtered_words_list.append(seg.word)
        return filtered_words_list
    for row in partition:
        sentence = re.sub("<.?>","",row.content) #替换掉标签数据
        words = cut_sentence(sentence)
        yield row.id,row.region,row.position_category,words
```

​	pyspark调用函数，对每个partition执行分词：

```python
word_df = position_dataframe.rdd.mapPartitions(segmentation).toDF(["position_id", "region", "industry", "content"])
```

### 2、计算TF-IDF

​	计算TF-IDF（词频－逆向文件频率）是一种常用的文本分析方法，TF-IDF通过计算词语在文档和所有文档中的出现频率，可以体现文档中某个词语在整个语料库的重要程度。

五、建立职位画像和用户画像

​	

