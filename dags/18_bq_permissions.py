from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator


DATASET = "bigquery-public-data.geo_international_ports"
TABLE = "world_port_index"

default_args = {"start_date": datetime(2022, 4, 1), "retry_delay": timedelta(seconds=1)}

with DAG(
    "19_bigquery_permissions", default_args=default_args, schedule_interval=None
) as dag:
    check_count = BigQueryCheckOperator(
        task_id="check_count",
        gcp_conn_id="gcp_connection_2",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        use_legacy_sql=False,
    )