#coding:utf8
#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import jieba
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("dwd")\
        .config("spark.sql.shuffle.partitions",24)\
        .config("spark.num.executors",2)\
        .config("spark.executor.cores",3)\
        .config("spark.default.parallelism",12) \
	    .config("spark.executor.memory","800m") \
        .config("spark.yarn.am.memory","512m")\
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("use ods")
    #  动态接收 Airflow 传进来的分区日期，默认值改为 20260506--用于测试
    dt_value = sys.argv[1] if len(sys.argv) > 1 else "20260506"
    
    #获取每日新增数据,并对事实表去除重复值和空值
    user_be = spark.sql(f"select user_id,category_id,behavior,sex,address,device \
    from user_behavior_inc where dt ='{dt_value}'").dropDuplicates().dropna(subset=['user_id', 'category_id'])
    user_face = spark.sql("select * from ods_user_face_full")
    goods = spark.sql("select category_id,category_label from ods_category_mapping_full")
    user_com = spark.sql(f"select user_id,category_id,comment from user_comment_inc where dt = '{dt_value}'")\
        .dropDuplicates().dropna(subset=['user_id',"category_id"])
    #数据清洗
    # join各表
    #行为大表
    user_behavior = user_be.join(user_face,on='user_id',how='left').join(goods,on=['category_id'],how='left')
    #评论大表
    user_comment = user_com.join(user_face,on='user_id',how='left').join(goods,on=['category_id'],how='left')

    #评论转化--可扩展
    positive_words = {"好", "不错", "喜欢", "清晰", "快", "赞", "满意", "值得", "很多", "好吃"}
    negative_words = {"差", "坏", "慢", "卡", "失望", "退货", "垃圾", "不行", "扔", "乱", "难吃", "故障", "说法", "难","返修", "变质"}
    #创建广播变量
    broadcast_pos = spark.sparkContext.broadcast(positive_words)
    broadcast_neg = spark.sparkContext.broadcast(negative_words)

    def analyze_sentiment(comment):
        if not comment:
            return 0
        #引入词典
        pos_words = broadcast_pos.value
        neg_words = broadcast_neg.value
        # 词频统计
        words = jieba.lcut(comment)
        score = 0
        for word in words:
            if word in pos_words:
                score += 1
            elif word in neg_words:
                score -= 1
        # 打标
        if score >=1 :
            return 1
        else :
            return 0

    #注册为UDF函数
    analyze_sentiment = F.udf(analyze_sentiment, IntegerType())
    #更新评论
    user_comment = user_comment.withColumn("comment", analyze_sentiment(F.col("comment")))
    #挂载无划线格式的 dt 分区字段
    user_behavior_final = user_behavior.withColumn("dt", F.lit(dt_value))
    user_comment_final = user_comment.withColumn("dt", F.lit(dt_value))

    #对齐字段顺序
    user_behavior_ordered = user_behavior_final.select(
        "user_id","category_id", "behavior","sex", "address", "device", "face", "category_label", "dt")


    user_comment_ordered = user_comment_final.select(
        "user_id", "category_id", "comment","face", "category_label", "dt")

    #写入到DWD层数据---都使用覆盖写
    user_behavior_final.write \
        .mode("overwrite") \
        .format("orc") \
        .insertInto("dwd.dwd_user_behavior")

    user_comment_final.write \
        .mode("overwrite") \
        .format("orc") \
        .insertInto("dwd.dwd_user_comment")
