"""
vars:
- path to countries data (s3?)
- 

1. check for data in path
2. get files modified since last successful run
3. extract data
    - filter out existing data? or overwrite / cumulative approach?
    - instantiate models based on the raw data
        - pydantic models having only the properties we're interested in
    - dump model to json one liner >> output.ndjson
    
    
dump structure:

2020
  - AT
    - 20210122T121314+3_buyers.ndjson
    - 20210122T121314+3_sellers.ndjson
    - 20210122T121314+3_tenders.ndjson
    - 20220423T150105+2_buyers.ndjson
    - 20220423T150105+2_sellers.ndjson
    - 20220423T150105+2_tenders.ndjson
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta

RAW_SOURCE = Variable.get("RAW_SOURCE", default_var='/data/raw/')


def my_custom_function(ts,**kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(f"I am task number {kwargs['task_number']}. This DAG Run execution date is {ts} and the current time is {datetime.now()}")
    print('Here is the full DAG Run context. It is available because provide_context=True')
    print(kwargs)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('extract_vertices',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval="@monthly",
         default_args=default_args,
         catchup=True # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    t1 = DummyOperator(
        task_id='group_bash_tasks'
    )
    t2 = BashOperator(
        task_id='bash_print_date1',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')
    t3 = BashOperator(
        task_id='bash_print_date2',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')

    # generate tasks with a loop. task_id must be unique
    for task in range(5):
        if version.startswith('2'):
            tn = PythonOperator(
                task_id=f'python_print_date_{task}',
                python_callable=my_custom_function,  # make sure you don't include the () of the function
                op_kwargs={'task_number': task},
            )
        else:
            tn = PythonOperator(
                task_id=f'python_print_date_{task}',
                python_callable=my_custom_function,  # make sure you don't include the () of the function
                op_kwargs={'task_number': task},
                provide_context=True,
            )


        t0 >> tn # indented inside for loop so each task is added downstream of t0

    t0 >> t1
    t1 >> [t2, t3] # lists can be used to specify multiple tasks