"""
1. get all links from digiwhist
2. check what's new, make list of urls to download from
3. download (zipped) csv files
4. extract, name, tag and move the collected files to their designed location


storage bucket structure:

2020
  - AT
    - 20220423T150105+2.csv
    - 20220424T191155+2.csv
    - ...
  - BE
  - ...
  - ZA
"""

from os import walk
from pathlib import Path

from tempfile import NamedTemporaryFile, TemporaryDirectory
import zipfile
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import boto3
from botocore.client import Config
from datetime import datetime, timedelta

import requests

BASE_URL = "https://opentender.eu/data/files"
ZIP_BUCKET = 'digiwhist'
CSV_BUCKET = 'digiwhist'

COUNTRY_CODES = ['ro', 'sk']

s3 = None

def __get_s3():
    global s3
    if s3 is None:
        s3 = boto3.resource('s3',
                            endpoint_url=Variable.get('S3_ENDPOINT', 'https://s3.0x19.net'),
                            aws_access_key_id=Variable.get('MINIO_ACCESS_KEY_ID'),
                            aws_secret_access_key=Variable.get('MINIO_SECRET_ACCESS_KEY'),
                            config=Config(signature_version='s3v4'),
                            region_name='eu-central')
    return s3

def download_raw_data(codes: list):
    for code in codes:
        url = f"{BASE_URL}/data-{code}-csv.zip"
        print(f"Getting data for {code.upper()}... ", end='')
        r = requests.get(url, allow_redirects=True)
        print(f"done.")

        with NamedTemporaryFile() as tmpfile:
            tmpfile.write(r.content)
            bucket = __get_s3().Bucket(ZIP_BUCKET)
            bucket.upload_file(str(tmpfile.name), f"zip/data-{code}-csv.zip")

              
def s3_zip_to_csv(codes: list):
    for code in codes:
        url = f"{BASE_URL}/data-{code}-csv.zip"
        r = requests.get(url, allow_redirects=True)

        with TemporaryDirectory() as tmpdir:
            with NamedTemporaryFile() as tmpfile:            
                zip_bucket = __get_s3().Bucket(ZIP_BUCKET)
                zip_bucket.download_file(f'zip/data-{code}-csv.zip', tmpfile.name)
                with zipfile.ZipFile(tmpfile.name, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)
                    
                pathlist = Path(tmpdir).glob('**/*.csv')
                for path in pathlist:
                    csv_bucket = __get_s3().Bucket(ZIP_BUCKET)
                    csv_bucket.upload_file(str(path), f"csv/{str(path).split('/')[-1]}")


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('collect_raw',
         start_date=datetime(2022, 4, 4),
         max_active_runs=3,
         schedule_interval="@monthly",
         default_args=default_args,
         catchup=True # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = PythonOperator(
                task_id=f'download_zip',
                python_callable=download_raw_data,
                op_args=[COUNTRY_CODES],
            )

    t1 = PythonOperator(
                task_id=f'extract_zip',
                python_callable=s3_zip_to_csv,
                op_args=[COUNTRY_CODES],
            )

    t0 >> t1
