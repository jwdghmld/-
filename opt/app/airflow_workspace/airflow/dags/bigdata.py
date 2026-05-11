#!/opt/app/miniforge3/envs/airflow/bin/python
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

default = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["你的邮箱"],
    "start_date": datetime(2026, 5, 7),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG('bigdata', default_args=default, schedule_interval="@daily", catchup=False) as dag:

    t1_to_mysql1 = BashOperator(
        task_id="first1",
        bash_command="""
        # 让 DataX 读取导入到mysql
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/temp1.json

        #将数据覆盖写入到hdfs中
        /opt/app/datax/bin/datax.py /opt/app/datax/job/tohdfs1.json
        """,
        retries=1,
        dag=dag)

    t1_to_mysql2 = BashOperator(
        task_id="first2",
        bash_command="""
        # 让 DataX 读取导入到mysql
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/temp2.json

        #将数据覆盖写入到hdfs中
        /opt/app/datax/bin/datax.py /opt/app/datax/job/tohdfs2.json
        """,
        retries=1,
        dag=dag)

    # 将数据写入到ODS层
    t2_hive1_ods = HiveOperator(
        task_id="ODS1",
        hive_cli_conn_id="hive",
        hql="""  
                use ods;
                load data inpath '/input-b/*' overwrite into table  user_behavior_tmp partition(dt='{{ ds_nodash }}');
                insert into table ods.user_behavior_inc select * from ods.user_behavior_tmp where dt ='{{ ds_nodash }}';
            """,
        dag=dag)

    t2_hive2_ods = HiveOperator(
    	task_id="ODS2",
    	hive_cli_conn_id="hive",
    	hql="""
                use ods;
                load data inpath '/input-c/*' overwrite into table  user_comment_tmp partition(dt='{{ ds_nodash }}');
                insert into table user_comment_inc select * from user_comment_tmp where dt = '{{ ds_nodash }}';
                """,
    	dag=dag)

    # 写入到DWD
    t3_spark_dwd = BashOperator(
    		task_id='spark_dwd',
    		bash_command="""
       	 	/opt/app/spark-3.5.8/bin/spark-submit --master yarn --driver-memory 1g /home/dage/py_data/dwd.py {{ ds_nodash }}
        	""",
    		dag=dag)

    #写入到DWS---日常
    t4_spark_dws1 = BashOperator(
        task_id='spark_dws-日常',
        bash_command="""
           	/opt/app/spark-3.5.8/bin/spark-submit --master yarn --driver-memory 1g /home/dage/py_data/dws1.py {{ ds_nodash }}
            	""",
        dag=dag)

    t4_spark_dws2 = BashOperator(
        task_id='spark_dws-全量',
        bash_command="""
           	/opt/app/spark-3.5.8/bin/spark-submit --master yarn --driver-memory 1g /home/dage/py_data/dws2.py {{ ds_nodash }}
            	""",
        dag=dag)

    #写入到ADS
    t5_spark_dwd = BashOperator(
        task_id='spark_ads',
        bash_command="""
           	 /opt/app/spark-3.5.8/bin/spark-submit --master yarn --driver-memory 1g /home/dage/py_data/ads.py {{ ds_nodash }}
            	""",
        dag=dag)


    #写入到mysql
    t6_hive_to_mysql = BashOperator(
        task_id="result_to_mysql",
        bash_command="""
        # 让 DataX 读取hive 并写入到mysql中
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/goods_pop.json
        
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/goods_score.json
        
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/face_favorite.json
        
        /opt/app/datax/bin/datax.py -p "-Ddt={{ ds_nodash }}" /opt/app/datax/job/goods_pv.json
        """,
        retries=1,
        dag=dag)

    email = EmailOperator(
    	task_id="eamil",
    	to='你的邮箱',
    	subject='主题 ',
    	html_content='<h1> 实际内容 </h1>',
    	cc='你的邮箱',
    	dag=dag)

    t1_to_mysql1 >> t2_hive1_ods >> t1_to_mysql2 >> t2_hive2_ods >> t3_spark_dwd >> t4_spark_dws1 >> t4_spark_dws2 >> t5_spark_dwd >> t6_hive_to_mysql >> email
