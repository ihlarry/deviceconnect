import os
from datetime import date, datetime, timedelta
import logging

from flask import Blueprint, request
from google.cloud import bigquery



log = logging.getLogger(__name__)


bp = Blueprint("fitbit_query_bp", __name__)

bigquery_datasetname = os.environ.get("BIGQUERY_DATASET")
if not bigquery_datasetname:
    bigquery_datasetname = "fitbit2"

client = bigquery.Client()

def _tablename(table: str) -> str:
    return bigquery_datasetname + "." + table


@bp.route("/find_step_thresholds")
def find_step_thresholds():
    # Construct a BigQuery client object.
    yesterday = datetime.now() - timedelta(1)

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination="pericardits.fitbit.step_alerts")
    job_config.write_disposition = 'WRITE_APPEND'
    query = """
        SELECT *
        FROM `pericardits.fitbit.intraday_steps`
        WHERE value < 10000
        AND date = @yesterday;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("yesterday", "STRING", datetime.strftime(yesterday, '%Y-%m-%d')),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return "Done"