import datetime
import time
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'archival',
    default_args={
        'retries': 0,
    },
    description='Archives data from Clickhouse to GCP/AWS',
    schedule_interval=datetime.timedelta(hours=1),
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
) as dag:

    start = datetime.datetime.now() - datetime.timedelta(hours=1)
    end = datetime.datetime.now()

    start_ns = time.mktime(start.timetuple()) * 1000 * 1000 * 1000
    end_ns = time.mktime(end.timetuple()) * 1000 * 1000 * 1000

    command = '/root/spark-3.2.1-bin-hadoop3.2/bin/spark-submit   --master k8s://https://10.96.11.2   --deploy-mode cluster   --conf spark.driver.START_TIME={0} --conf spark.driver.END_TIME={1} --conf spark.driver.JDBC_URL="jdbc:clickhouse://34.132.183.236:8123" --conf spark.driver.DB_USER="clickhouse_operator" --conf spark.driver.DB_PASSWORD="clickhouse_operator_password" --conf spark.driver.DB_NAME="opsramp" --conf spark.executor.instances=5   --conf spark.kubernetes.container.image=docker.io/veeralpatel123/archival@sha256:dd1197dc3194e56c3f4722bbafb3ffc98a91b4d71417938d10b4cbe62402be74   local:///opt/tracing/cmd/archival/etl.py'.format(start_ns, end_ns)

    t1 = BashOperator(
        task_id='run',
        bash_command=command
    )

    t1
