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
    dt_obj = datetime.strptime(dt_value, '%Y%m%d')
    yesterday = (dt_obj - timedelta(days=1)).strftime('%Y%m%d')

    spark.sql("use dws")
    #全量表更新
    # 1. 职业行为全量
    try:
        spark.sql(f"select face,category_label,pv_count,cart_count,buy_count from dws_face_full where dt = '{yesterday}'").createTempView('b1')
    except Exception as e:
        spark.sql(f"select 'dummy' as face,'dummy' as category_label, 0L as pv_count, 0L as cart_count, 0L as buy_count from dws_face_day limit 0").createOrReplaceTempView("b1")

    beh_full = spark.sql(f"""
        select face,category_label, 
               sum(pv_count) as pv_count, 
               sum(cart_count) as cart_count, 
               sum(buy_count) as buy_count
        from (
            select face,category_label, pv_count, cart_count, buy_count from b1
            union all
            select face,category_label, pv_count, cart_count, buy_count from dws_face_day where dt = '{dt_value}'
        ) t
        group by face,category_label
            """)

    beh_full.withColumn('dt',F.lit(dt_value)). \
        select('face','category_label','pv_count','cart_count','buy_count','dt') .\
        write.mode('overwrite').format('orc').insertInto('dws.dws_face_full')

    # 2. 商品流量统计全量
    try:
        spark.sql(f"select * from dws_goods_full where dt = '{yesterday}'").createTempView('g1')
    except Exception as e:
        spark.sql(f"select 'dummy' as category_label, 0L as total_pv, 0L as total_cart, 0L as total_buy from dws_goods_day limit 0").createOrReplaceTempView("g1")

    goods_full = spark.sql(f"""
        select category_label, 
               sum(total_pv) as total_pv, 
               sum(total_cart) as total_cart, 
               sum(total_buy) as total_buy
        from (
            select category_label, total_pv, total_cart, total_buy from g1
            union all
            select category_label, total_pv, total_cart, total_buy from dws_goods_day where dt = '{dt_value}'
        ) t
        group by category_label
            """)

    goods_full.withColumn('dt',F.lit(dt_value)). \
        select('category_label', 'total_pv', 'total_cart', 'total_buy', 'dt') .\
        write.mode('overwrite').format('orc').insertInto('dws.dws_goods_full')

    # 3. 商品口碑统计全量
    try:
        spark.sql(f"select * from dws_goods_reputation_full where dt = '{yesterday}'").createTempView('c1')
    except Exception as e:
        spark.sql(f"select 'dummy' as category_label, 0L as total_comments, 0L as good_count, 0L as bad_count from dws_goods_reputation_day limit 0").createOrReplaceTempView("c1")

    com_full = spark.sql(f"""
        select category_label, 
               sum(total_comments) as total_comments, 
               sum(good_count) as good_count, 
               sum(bad_count) as bad_count
        from (
            select category_label, total_comments, good_count, bad_count from c1
            union all
            select category_label, total_comments, good_count, bad_count from dws_goods_reputation_day where dt = '{dt_value}'
        ) t
        group by category_label
            """)

    com_full.withColumn('dt',F.lit(dt_value)). \
        select('category_label','total_comments','good_count','bad_count','dt') .\
        write.mode('overwrite').format('orc').insertInto('dws.dws_goods_reputation_full')

    spark.stop()
