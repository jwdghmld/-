use bigdata;
#用户行为日增表，只保留每日最新数据
create table behavior_daily (
    user_id int comment "用户id",
    goods_id int comment "商品具体id",
    category_id int comment "商品品类id",
    behavior char(5) comment "用户行为",
    `timestamp` int comment "uninx code时间",
    sex int comment "性别",
    address varchar(20) comment "省份",
    device varchar(20) comment "手机类型",
    price int comment "商品价格",
    amount int comment "购买数量"
) comment "用户行为表";
#商品评论表，只保留每日最新数据
create table comment_daily (
    user_id int comment "用户id",
    goods_id int comment "商品具体id",
    category_id int comment "商品品类id",
    `comment` varchar(1000) comment "用户评论"
) comment "用户评论表";


-- bigdata
-- 1. 当日最受欢迎商品表 (来源ads_goods_pop)
-- 业务场景：展示全站每日/历史累计的人气排行 Top 100
CREATE TABLE IF NOT EXISTS goods_pop (
  category_label VARCHAR(255) NOT NULL COMMENT '商品类目',
  score DOUBLE DEFAULT NULL COMMENT '人气加权总得分',
  rk INT DEFAULT NULL COMMENT '排名',
  dt VARCHAR(20) NOT NULL COMMENT '分区日期',
  PRIMARY KEY (dt, category_label) -- 联合主键：防止同一天同一个类目重复插入
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='最受欢迎商品排行表';

-- 2. 各职业中最受欢迎商品表 (来源ads_face_favorite)
-- 业务场景：精准推荐，查看不同职业人群的消费偏好
CREATE TABLE IF NOT EXISTS face_favorite (
  face VARCHAR(100) NOT NULL COMMENT '用户职业/画像',
  category_label VARCHAR(255) NOT NULL COMMENT '商品类目',
  score DOUBLE DEFAULT NULL COMMENT '人气加权总得分',
  rk INT DEFAULT NULL COMMENT '排名',
  dt VARCHAR(20) NOT NULL COMMENT '分区日期',
  PRIMARY KEY (dt, face, category_label) -- 联合主键：日期+职业+类目
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='职业偏好排行表';

-- 3. 转化漏斗表 (ads_goods_pv_to_buy)
-- 业务场景：评估商品的引流转化能力（威尔逊下限算法）
CREATE TABLE IF NOT EXISTS goods_pv_to_buy (
  category_label VARCHAR(255) NOT NULL COMMENT '商品类目',
  buy_rate DOUBLE DEFAULT NULL COMMENT '威尔逊转化率下限',
  dt VARCHAR(20) NOT NULL COMMENT '分区日期',
  PRIMARY KEY (dt, category_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品转化率榜单(威尔逊)';

-- 4. 商品口碑榜 (ads_goods_score)
-- 业务场景：供应链风控与商品优选（威尔逊下限算法）
CREATE TABLE IF NOT EXISTS goods_score (
  category_label VARCHAR(255) NOT NULL COMMENT '商品类目',
  good_rate DOUBLE DEFAULT NULL COMMENT '威尔逊好评率下限',
  dt VARCHAR(20) NOT NULL COMMENT '分区日期',
  PRIMARY KEY (dt, category_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品口碑榜单(威尔逊)';