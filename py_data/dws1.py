#coding:utf8
#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import timedelta,datetime
import sys

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("dws")\
        .config("spark.sql.shuffle.partitions",24)\
        .config("spark.num.executors",2)\
        .config("spark.executor.cores",3)\
        .config("spark.default.parallelism",12) \
        .config("spark.sql.files.maxPartitionBytes", "根据实际文件数据动态调整") \
        .config("spark.executor.memory","800m")\
        .config("spark.yarn.am.memory","512m")\
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .enableHiveSupport()\
        .getOrCreate()
    #  动态接收 Airflow 传进来的分区日期和最日日期
    dt_value = sys.argv[1] if len(sys.argv) > 1 else "20260506"
    
    spark.sql("use dwd")
    #用户行为统计表
    beh = spark.sql(f"select face,category_label,behavior from dwd_user_behavior where dt = '{dt_value}'")
    #商品流行统计表
    goods = spark.sql(f"select category_label,behavior from dwd_user_behavior where dt = '{dt_value}'")
    #商品评论统计表
    com = spark.sql(f"select category_label,comment from dwd_user_comment where dt = '{dt_value}'")

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
    com = com.withColumn("comment", analyze_sentiment(F.col("comment")))
	

    #二次聚合
    #  一、用户行为
    # 1. 局部聚合 (加盐)：给 goods_id 加上 0~9 的随机前缀，强行打散热点
    beh_salted = beh.withColumn("face", F.concat(F.lit(F.round(F.rand() * 9)).cast("int"), F.lit("_"), F.col("face")))
    beh_salted.createTempView("beh1")
    beh1 = spark.sql("select face,category_label,\
        sum(if(behavior='pv',1,0)) as pv_count,\
        sum(if(behavior='cart',1,0)) as cart_count,\
        sum(if(behavior='buy',1,0)) as buy_count\
        from beh1 group by face,category_label")
    # 2. 全局聚合 (去盐)：把随机前缀切掉，再做一次 SUM
    beh2 = beh1.withColumn("face", F.split(F.col("face"), "_")[1])
    beh2.createTempView("beh2")
    beh_result = spark.sql("select face,category_label,\
        sum(pv_count) as pv_count,\
        sum(cart_count) as cart_count,\
        sum(buy_count) as buy_count\
        from beh2 group by face,category_label")

    #  二、商品流行统计
    # 1. 局部加盐
    goods_salted = goods.withColumn("category_label", F.concat(F.lit(F.round(F.rand() * 9)).cast('int'), F.lit("_"), F.col("category_label")))
    goods_salted.createTempView("g1")
    g1 = spark.sql("select category_label,\
        sum(if(behavior='pv',1,0)) as total_pv,\
        sum(if(behavior='cart',1,0)) as total_cart,\
        sum(if(behavior='buy',1,0)) as total_buy\
        from g1 group by category_label")
    # 2. 全局聚合
    g2 = g1.withColumn("category_label", F.split(F.col("category_label"), "_")[1])
    g2.createTempView("g2")
    goods_result = spark.sql("select category_label,\
        sum(total_pv) as total_pv,\
        sum(total_cart) as total_cart,\
        sum(total_buy) as total_buy \
        from g2 group by category_label")

    # 三、 商品评论统计
    # 1. 局部加盐
    com_salted = com.withColumn("category_label", F.concat(F.lit(F.round(F.rand() * 9)).cast("int"), F.lit("_"), F.col("category_label")))
    com_salted.createTempView("c1")
    c1 = spark.sql("select category_label,\
        count(*) as total_comments,\
        sum(comment) as good_count,\
        count(*) - sum(comment) as bad_count \
        from c1 group by category_label")
    # 2. 全局聚合
    c2 = c1.withColumn("category_label", F.split(F.col("category_label"), "_")[1])
    c2.createTempView("c2")
    com_result = c2.groupBy('category_label').agg(F.sum('total_comments').alias('total_comments'),\
                                                  F.sum('good_count').alias('good_count'),\
                                                  F.sum('bad_count').alias('bad_count'))

    #添加分区值,并对齐分区值
    beh_final = beh_result.withColumn("dt", F.lit(dt_value)).\
        select('face','category_label','pv_count','cart_count','buy_count','dt')
    com_final = com_result.withColumn("dt", F.lit(dt_value)).\
        select('category_label','total_comments','good_count','bad_count','dt')
    goods_final = goods_result.withColumn("dt", F.lit(dt_value)). \
        select('category_label', 'total_pv', 'total_cart', 'total_buy', 'dt')

    #写入到DWS层每日数据---都使用覆盖写
    beh_final.write \
        .mode("overwrite") \
        .format("orc") \
        .insertInto("dws.dws_face_day")

    goods_final.write \
        .mode("overwrite") \
        .format("orc") \
        .insertInto("dws.dws_goods_day")

    com_final.write \
        .mode('overwrite') \
        .format('orc') \
        .insertInto('dws.dws_goods_reputation_day')

    spark.stop()
