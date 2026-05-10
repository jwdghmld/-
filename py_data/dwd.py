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

    #评论转化
    def analyze_sentiment(comment):
        if not comment:
            return 0
        #自定义词典---基本
        positive_words = {
    		"好", "不错", "喜欢", "清晰", "快", "赞", "满意", "值得", "很多", "好吃",
   	 	"优秀", "完美", "精致", "实惠", "好用", "耐用", "方便", "舒服", "好看",
    		"漂亮", "高端", "大气", "时尚", "顺滑", "流畅", "省电", "安静", "小巧",
    		"轻便", "强劲", "大屏", "鲜艳", "逼真", "灵敏", "工整", "细致", "贴心",
    		"惊喜", "超值", "推荐", "回购", "放心", "靠谱", "周到", "热情", "专业",
    		"新鲜", "厚实", "柔软", "保暖", "透气"
		}
        negative_words = {
    		"差", "坏", "慢", "卡", "失望", "退货", "垃圾", "不行", "扔", "乱",
    		"难吃", "故障", "说法", "难", "返修", "变质", "差评", "坑", "假",
    		"破损", "划痕", "掉色", "缩水", "起球", "变形", "异味", "噪音", "发热",
    		"迟钝", "模糊", "闪退", "死机", "卡顿", "延迟", "耗电", "漏水", "漏气",
    		"粗糙", "劣质", "廉价", "敷衍", "蛮横", "推诿", "过期", "发霉", "生锈",
    		"开裂", "脱落", "刺激", "过敏", "短命"
		}
        # 词频统计
        words = jieba.lcut(comment)
        score = 0
        for word in words:
            if word in positive_words:
                score += 1
            elif word in negative_words:
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
