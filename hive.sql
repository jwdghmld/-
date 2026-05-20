--初始数据表  用于临时存储数据

--1.建立ODS
drop database if exists ods cascade;

CREATE DATABASE IF NOT EXISTS ods;
USE ods;

-- =======临时数据表======

-- 用于分桶和分区

-- 1. 用户行为临时表
CREATE EXTERNAL TABLE IF NOT EXISTS ods.user_behavior_tmp (
    user_id     STRING COMMENT '用户ID',
    goods_id    STRING COMMENT '商品ID',
    category_id STRING COMMENT '类目ID',
    behavior    STRING COMMENT '行为类型:pv,cart,fav,buy',
  `timestamp`   BIGINT COMMENT '行为时间戳',
    sex         STRING COMMENT '性别',
    address     STRING COMMENT '地域',
    device      STRING COMMENT '设备',
    price       DECIMAL(10,2) COMMENT '单价',
    amount      INT COMMENT '数量'
)
COMMENT '用户行为临时流水表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 2. 评论事实临时表
CREATE EXTERNAL TABLE IF NOT EXISTS ods.user_comment_tmp(
    user_id       STRING COMMENT '用户ID',
    goods_id      STRING COMMENT '商品ID',
    category_id   STRING COMMENT '类目ID',
    `comment`     STRING COMMENT '评论内容'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- ======实际事实表，动态分区和分桶======

-- 用户行为事实表
CREATE EXTERNAL TABLE IF NOT EXISTS ods.user_behavior_inc (
    user_id     STRING COMMENT '用户ID',
    goods_id    STRING COMMENT '商品ID',
    category_id STRING COMMENT '类目ID',
    behavior    STRING COMMENT '行为类型:pv,cart,fav,buy',
  `timestamp`   BIGINT COMMENT '行为时间戳',
    sex         STRING COMMENT '性别',
    address     STRING COMMENT '地域',
    device      STRING COMMENT '设备',
    price       DECIMAL(10,2) COMMENT '单价',
    amount      INT COMMENT '数量'
)
COMMENT '用户行为流水表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
CLUSTERED BY (user_id,category_id) SORTED BY (user_id,category_id) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC;

--评论事实表
CREATE EXTERNAL TABLE IF NOT EXISTS ods.user_comment_inc (
    user_id       STRING COMMENT '用户ID',
    goods_id      STRING COMMENT '商品ID',
    category_id   STRING COMMENT '类目ID',
    `comment`     STRING COMMENT '评论内容'
)
PARTITIONED BY (dt STRING)
CLUSTERED BY (category_id) SORTED BY (category_id) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC;

-- 用户画像维表 (按 user_id 分桶)
CREATE TABLE IF NOT EXISTS ods.ods_user_face_full (
    user_id STRING COMMENT '用户ID',
    face    STRING COMMENT '画像/职业标签'
)
COMMENT '用户画像维表'
CLUSTERED BY (user_id) SORTED BY (user_id)  INTO 8 BUCKETS
STORED AS ORC;

-- 商品类目维表 (按 category_id 分桶)
CREATE TABLE IF NOT EXISTS ods.ods_category_mapping_full (
    category_id    STRING COMMENT '类目ID',
    category_count INT    COMMENT '类目下商品数',
    category_label STRING COMMENT '类目名称'
)
COMMENT '商品类目映射表'
CLUSTERED BY (category_id) SORTED BY (category_id) INTO 4 BUCKETS
STORED AS ORC;

--ods维度表---全量作为中转
-- 用户画像维表
CREATE TABLE IF NOT EXISTS ods.face (
    user_id STRING COMMENT '用户ID',
    face    STRING COMMENT '画像/职业标签'
)
COMMENT '用户画像维表' row format delimited fields terminated by ',';

-- 商品类目维表
CREATE TABLE IF NOT EXISTS ods.category(
    category_id    STRING COMMENT '类目ID',
    category_count INT    COMMENT '类目下商品数',
    category_label STRING COMMENT '类目名称'
)
COMMENT '商品类目映射表' row format delimited fields terminated by ',';

load data local inpath 'data/user_face.csv' overwrite into table ods.face;
load data local inpath 'data/category_mapping.csv' overwrite into table ods.category;

insert into table ods.ods_category_mapping_full select * from ods.category;
insert into table ods.ods_user_face_full select * from ods.face;

--DWD
drop database if exists dwd cascade;
CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

-- DWD: 用户行为明细宽表 (日增量)
CREATE TABLE IF NOT EXISTS dwd.dwd_user_behavior(
    user_id          STRING COMMENT '用户ID',
    category_id      STRING COMMENT '类目ID',
    behavior         STRING COMMENT '行为类型:pv,cart,fav,buy',
    sex              STRING COMMENT '性别',
    address          STRING COMMENT '地域',
    device           STRING COMMENT '设备',
    face             STRING COMMENT '用户职业标签 (来自维表)',
    category_label   STRING COMMENT '商品类目名称 (来自维表)'
)
COMMENT '用户行为明细宽表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

--  用户评论明细宽表 (日增量)
CREATE TABLE IF NOT EXISTS dwd.dwd_user_comment(
    user_id          STRING COMMENT '用户ID',
    category_id      STRING COMMENT '类目ID',
  `comment`          STRING COMMENT '评论内容',
    face             STRING COMMENT '用户职业标签 (来自维表)',
    category_label   STRING COMMENT '商品类目名称 (来自维表)'
)
COMMENT '用户评论明细宽表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 1. 创建并切换到 DWS 数据库
drop database if exists dws cascade;
CREATE DATABASE IF NOT EXISTS dws;
USE dws;

-- ==========================================
-- 主题一：人群偏好日汇总表 (交叉维度)
-- 业务场景：各职业人群每天最喜欢看/买什么商品
-- ==========================================
CREATE TABLE IF NOT EXISTS dws.dws_face_day (
    face            STRING COMMENT '用户职业画像',
    category_label        STRING COMMENT '商品名称',
    pv_count        BIGINT COMMENT '该职业对该商品的当日总浏览量',
    cart_count      BIGINT COMMENT '该职业对该商品的当日总加购量',
    buy_count       BIGINT COMMENT '该职业对该商品的当日总购买量'
)
COMMENT '人群商品偏好日汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 全量表
CREATE TABLE IF NOT EXISTS dws.dws_face_full (
    face            STRING COMMENT '用户职业画像',
    category_label        STRING COMMENT '商品名称',
    pv_count        BIGINT COMMENT '该职业对该商品的总浏览量',
    cart_count      BIGINT COMMENT '该职业对该商品的总加购量',
    buy_count       BIGINT COMMENT '该职业对该商品的总购买量'
)
COMMENT '人群商品偏好全汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- ==========================================
-- 主题二：商品流量销量日汇总表 (单维度)
-- 业务场景：全站每日商品热销榜、流量漏斗分析
-- ==========================================
CREATE TABLE IF NOT EXISTS dws.dws_goods_day (
    category_label  STRING COMMENT '商品类目名称(保留维度方便下游查看)',
    total_pv        BIGINT COMMENT '该商品当日总浏览量',
    total_cart      BIGINT COMMENT '该商品当日总加购量',
    total_buy       BIGINT COMMENT '该商品当日总购买量'
)
COMMENT '商品流量销量日汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
-- 全量表
CREATE TABLE IF NOT EXISTS dws.dws_goods_full (
    category_label  STRING COMMENT '商品类目名称(保留维度方便下游查看)',
    total_pv        BIGINT COMMENT '该商品总浏览量',
    total_cart      BIGINT COMMENT '该商品总加购量',
    total_buy       BIGINT COMMENT '该商品总购买量'
)
COMMENT '商品流量销量全汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- ==========================================
-- 主题三：商品口碑日汇总表 (非结构化转结构化)
-- 业务场景：商品质量风控、差评预警、高分爆款挖掘
-- ==========================================
CREATE TABLE IF NOT EXISTS dws.dws_goods_reputation_day (
    category_label        STRING COMMENT '商品名称',
    total_comments  BIGINT COMMENT '当日总评论数',
    good_count      BIGINT COMMENT '当日好评数量',
    bad_count       BIGINT COMMENT '当日差评数量'
)
COMMENT '商品口碑日汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

--全量表
CREATE TABLE IF NOT EXISTS dws.dws_goods_reputation_full (
    category_label        STRING COMMENT '商品名称',
    total_comments  BIGINT COMMENT '总评论数',
    good_count      BIGINT COMMENT '好评数量',
    bad_count       BIGINT COMMENT '差评数量'
)
COMMENT '商品口碑全汇总表'
PARTITIONED BY (dt STRING COMMENT '日期分区,格式YYYYMMDD')
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


--建立ADS层
drop database if exists ads cascade;
CREATE DATABASE IF NOT EXISTS ads;
USE ads;

-- 1. 每日最受欢迎商品表 (引入加权分)
CREATE TABLE IF NOT EXISTS ads.ads_goods_pop (
    category_label  STRING COMMENT '商品类目',
    score DOUBLE COMMENT '人气加权总得分',
    rk int comment '排名'
) COMMENT '最受欢迎商品表'
PARTITIONED BY (dt STRING) STORED AS ORC;

-- 2. 各职业最受欢迎榜单
CREATE TABLE IF NOT EXISTS ads.ads_face_favorite (
    face            STRING COMMENT '职业',
    category_label  STRING COMMENT '商品类目',
    score DOUBLE COMMENT '人气加权总得分',
    rk              INT    COMMENT '排名'
) COMMENT '各职业最受好评商品Top10'
PARTITIONED BY (dt STRING) STORED AS ORC;

-- 3. 转化漏斗表 (引入威尔逊转化率下限)
CREATE TABLE IF NOT EXISTS ads.ads_goods_pv_to_buy (
    category_label  STRING COMMENT '商品类目',
    buy_rate DOUBLE COMMENT '威尔逊转化率下限'
) COMMENT '转化漏斗表(威尔逊)'
PARTITIONED BY (dt STRING) STORED AS ORC;

-- 4. 商品口碑榜 (引入威尔逊好评下限)
CREATE TABLE IF NOT EXISTS ads.ads_goods_score (
    category_label  STRING COMMENT '商品类目',
    good_rate DOUBLE COMMENT '威尔逊好评率下限'
) COMMENT '商品口碑榜(威尔逊)'
PARTITIONED BY (dt STRING) STORED AS ORC;
