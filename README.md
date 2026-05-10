🚀 斩月电商大数据平台：从数据采集到深度分析的全链路数仓实战
本项目构建了一个企业级的电商离线数仓系统，涵盖了从原始日志采集、清洗、多层建模到最终指标导出的全流程。通过对 4000万级（2GB+） 数据的压测，验证了集群在高并发计算下的稳定性与极致性能。

🏗️ 1. 数仓建模思路与业务背景本项目遵循 维度建模（Dimensional Modeling） 理论，
采用自下而上的四层架构（ODS -> DWD -> DWS -> ADS）。
📐 建表策略详解为了平衡查询性能与存储效率，本项目在建表时采用了以下优化手段：
存储格式： 全线采用 ORC 格式存储。相比 TextFile，ORC 提供的列式存储和索引能提升 Spark 约 3-5 倍的查询速度，并大幅降低 HDFS 空间占用。
分区设置（Partitioning）： 统一以 dt (日期) 作为一级分区字段，实现动态分区加载，避免全表扫描，满足 ODS 每日增量同步的需求。
分桶设计（Bucketing）： * 在 DWD 层核心表针对 user_id 进行分桶。
核心意义： 预先打散数据，使得在后续 DWS 层的 Join 或 Group By 操作中，Spark 可以实现 Bucket-Pruning 和 Sort-Merge Join，彻底消除大规模 Shuffle 带来的性能损耗。

⚙️ 2. 集群环境与技术栈配置本项目采用了严格的环境隔离方案，通过 Miniforge3 实现了计算引擎与调度引擎的 Python 环境解耦。
组件版本说明
    组件                说明
操作系统Rocky9   Linux 企业级 RHEL 系稳定内核
Hadoop3.3.6    分布式存储与 YARN 资源管理
Hive3.1.3      元数据管理与 ODS 接入
Spark3.5.8     核心计算引擎 (PySpark)  
Airflow2.5.1    全链路 DAG 任务调度DataX3.0异构数据同步 (HDFS ↔ MySQL)

🐍 Python 运行环境管理工具： Miniforge3 (Conda 兼容)
Spark 环境： Python 3.9 (兼顾计算库稳定性与 Spark 3.5 兼容性)
Airflow 环境： Python 3.10 (利用高版本 Python 提升调度器并发效率)

🔄 3. 任务流转与数据血缘整个 pipeline 通过 Airflow 编排，数据在各层间经历了从“脏数据”到“黄金指标”的蜕变。
L-M-H 环节 (Linux -> MySQL -> Hive)： 通过 DataX 将业务库数据抽取至 Hive ODS 层分区表。
ODS ➔ DWD (清洗层)： * 动作： 去重、空值过滤、用户画像 Join。产出： dwd_user_behavior (行为明细表)、dwd_user_comment (评论打标表)。
DWD ➔ DWS (汇总层)：策略： 同时进行增量聚合（当日指标）与全量聚合（历史快照合并）。技术点： 引入局部加盐（Salting）处理计算倾斜。
DWS ➔ ADS (应用层)： * 动作： 威尔逊下限算法、职业偏好加权计算。
ADS ➔ MySQL： 指标出库，供 Superset/Tableau 展示。

⚡ 4. 压测报告：4000万级数据性能表现测试集规模： 用户行为 (1GB) + 商品评论 (1GB) ≈ 40,000,000 条。
通过对 YARN 内存分配的深度优化（调整 executor.memory 为 800M，压低 maxPartitionBytes），集群展现了卓越的处理效率。

⏱️ 性能耗时清单 (总计约 19 分钟)
数据抽取环节 (L-M-H)： 15 min (瓶颈在于网络 IO 与 MySQL 读取)
清洗转换 (ODS -> DWD)： 60 s (Spark 向量化执行优势显现)
数据聚合 (DWD -> DWS)： 60 s (加盐策略成功化解数据倾斜)
深度建模 (DWS -> ADS)： 39 s (ORC 分桶读取加速查询)
结果出库 (ADS -> MySQL)： 49 s (DataX 多 Channel 并发写入)

🛡️ 5. 任务治理与任务监控
📧 邮件预警系统在 Airflow 中通过 EmailOperator 与 default_args 深度集成：
失败即通知： 任何任务实例失败，第一时间向管理员邮箱发送错误日志。
状态全闭环： 支持任务重启（Retry）通知及全量任务成功报告。

💾 集群内存调优针对 Rocky Linux 9 与 Spark 3.5.8 的特性，进行了以下资源适配：
动态申请： 开启 spark.dynamicAllocation.enabled，根据负载自动伸缩 Executor 数量。
并行度优化： 根据 YARN 虚拟核数比例调整 shuffle.partitions 为 24，确保 CPU 核心始终处于满载状态，绝无空闲等待。

💡 如何运行使用 Miniforge3 
创建两个独立的 conda 环境。
配置 airflow.cfg 中的 SMTP 服务以开启邮件提醒。
在 Airflow UI 中启用 bigdata_pipeline DAG。
