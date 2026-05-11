#coding:utf8
#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("dwd")\
        .config("spark.sql.shuffle.partitions",24)\
        .config("spark.num.executors",2)\
        .config("spark.executor.cores",3)\
        .config("spark.default.parallelism",12) \
	    .config("spark.executor.memory","800m") \
        .config("spark.yarn.am.memory","512m") \
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("use ods")
    # 动态接收日期
    dt_value = sys.argv[1] if len(sys.argv) > 1 else "20260506"

    # 1. 读取没有分区和分桶的纯临时表
    bt_tmp = spark.sql("select * from ods.user_behavior_tmp")
    ct_tmp = spark.sql("select * from ods.user_comment_tmp")

    #创建临时视图
    bt_tmp.createTempView("bt")
    ct_tmp.createTempView('ct')
    #写入日增表--静态分区
    # 1. 用户行为
    spark.sql(f"""
        insert overwrite table ods.user_behavior_inc partition(dt={dt_value})
        select user_id,goods_id,category_id,behavior,`timestamp`,sex,address,device,price,amount from bt 
        """)

    # 2. 用户评论
    spark.sql(f"""
        insert overwrite table ods.user_comment_inc partition(dt={dt_value})
        select user_id,goods_id,category_id,comment from ct 
        """)

    spark.stop()
