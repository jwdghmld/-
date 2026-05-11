#coding:utf8
#!/usr/bin/python3
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
import math
import sys


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("ads")\
        .config("spark.sql.shuffle.partitions",24)\
        .config("spark.num.executors",2)\
        .config("spark.executor.cores",3)\
        .config("spark.default.parallelism",12) \
        .config("spark.sql.files.maxPartitionBytes", "4194304") \
        .config("spark.executor.memory","800m")\
        .config("spark.yarn.am.memory","512m")\
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("use dws")
    #  动态接收 Airflow 传进来的分区日期，默认值改为 20260506--用于测试
    dt_value = sys.argv[1] if len(sys.argv) > 1 else "20260506"

    #威尔逊下限算法（z=1.96）
    def w(pos, total):
        # 预防除以 0 的情况
        if total == 0:
            return 0.0
        z = 1.96  # 95% 置信度
        phat = pos / total
        # 威尔逊公式
        inner_sqrt = (phat * (1 - phat)) / total + (z ** 2) / (4 * total ** 2)
        numerator = phat + (z ** 2) / (2 * total) - z * math.sqrt(max(0.0, inner_sqrt))
        denominator = 1 + (z ** 2) / total
        return round(numerator / denominator, 4)

    w = spark.udf.register('w',w,DoubleType())

    # 1. 当日最受欢迎商品表 -- 加权平均求和
    pop = spark.sql(f"""
        select * from(
        select category_label,score,row_number() over (order by score desc) as rk
        from (
            select category_label,round((total_pv + total_cart * 5 + total_buy * 10)/16.0,2) as score
            from dws.dws_goods_day where dt = '{dt_value}') as k1
        ) as k2
        where rk <= 100
        """)

    pop.withColumn('dt',F.lit(dt_value)).\
        select('category_label','score','rk','dt').\
        write.mode('overwrite').format('orc').insertInto('ads.ads_goods_pop')

    # 2. 各职业中最受欢迎商品表 -- 加权平均求和  --全量聚合
    fav = spark.sql(f"""
        select * from (
        select face,category_label,score,row_number() over (partition by face order by score desc) rk
        from (
            select face,category_label,
            round((pv_count + cart_count * 5 + buy_count * 10)/16.0,2) as score
            from dws.dws_face_full  where dt = '{dt_value}') as k1
        ) as k2
        where rk <= 3
        """)

    fav.withColumn('dt', F.lit(dt_value)).\
        select('face','category_label','score','rk','dt').\
        write.mode('overwrite').format('orc').insertInto('ads.ads_face_favorite')

    # 3. 转化漏斗表  浏览->购买的转化率  ---威尔逊下限法 w  --全量聚合
    funnel = spark.sql(f"""
        select category_label,
            w(total_buy,total_pv) as buy_rate
        from dws.dws_goods_full
        where dt = '{dt_value}'
        """)

    funnel.withColumn('dt', F.lit(dt_value)).\
        select('category_label','buy_rate','dt').\
        write.mode('overwrite').format('orc').insertInto('ads.ads_goods_pv_to_buy')

    # 4. 商品口碑榜  ---威尔逊下限法 w  --全量聚合
    sc = spark.sql(f"""
        select category_label,
               w(good_count, total_comments) as good_rate
        from dws.dws_goods_reputation_full
        where dt = '{dt_value}'
    """)

    sc.withColumn('dt', F.lit(dt_value)).\
        select("category_label","good_rate","dt") \
        .write.mode("overwrite").insertInto("ads.ads_goods_score")

    spark.stop()

