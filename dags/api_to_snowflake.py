import os
import requests
import snowflake.connector
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def fetch_api_data(**kwargs):
    """
    Fetch data from an API.
    """
    api_url = "https://www.healthit.gov/data/open-api?source=hospital-mu-public-health-measures.csv"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception("API call failed with status code {}".format(response.status_code))


def load_data_to_snowflake(**kwargs):
    """
    Load the fetched API data into Snowflake.
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_api_data')

    if not data or not isinstance(data, list) or len(data) == 0:
        raise Exception("No data received from API")

    # Retrieve Snowflake credentials from environment variables
    sf_user = os.environ.get("SNOWFLAKE_USER")
    sf_password = os.environ.get("SNOWFLAKE_PASSWORD")
    sf_account = os.environ.get("SNOWFLAKE_ACCOUNT")
    sf_database = os.environ.get("SNOWFLAKE_DATABASE")
    sf_schema = os.environ.get("SNOWFLAKE_SCHEMA")
    sf_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

    # Establish connection to Snowflake
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    cursor = conn.cursor()
    try:
        # Dynamically create table based on the first record's keys
        first_row = data[0]
        columns = first_row.keys()
        col_defs = ', '.join(['"{}" VARCHAR'.format(col) for col in columns])
        create_table_query = 'CREATE OR REPLACE TABLE HOSPITAL_MU_PUBLIC_HEALTH_MEASURES ({})'.format(col_defs)
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data into the table
        for row in data:
            cols = ', '.join(['"{}"'.format(col) for col in row.keys()])
            # Use string concatenation to avoid backslashes in f-string expressions
            vals = ', '.join("'" + str(val).replace("'", "''") + "'" for val in row.values())
            insert_query = 'INSERT INTO HOSPITAL_MU_PUBLIC_HEALTH_MEASURES ({}) VALUES ({})'.format(cols, vals)
            cursor.execute(insert_query)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'api_to_snowflake',
    default_args=default_args,
    description='Fetch data from an API and load into Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    task_fetch = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    task_load = PythonOperator(
        task_id='load_data_to_snowflake',
        python_callable=load_data_to_snowflake,
    )

    task_fetch >> task_load
