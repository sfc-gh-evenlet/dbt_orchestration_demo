import json
import os
import pendulum
import subprocess
import logging
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.decorators import task

default_args = {
    'email': ['eric.venlet@snowflake.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(
    dag_id="local_bash_operator_dbt_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jaffle_shop","dbt","bash"],
)
def jaffle_shop():

    # Retrieving connection detailsF
    connection = BaseHook.get_connection(conn_id='snowflake_db')
    password=connection.password
    user=connection.login

    dbt_vars = {
                    "dbt_commands":['build'], 
                    "profiles_dir":"/usr/local/airflow/dags/.dbt", 
                    "project_dir":"/usr/local/airflow/dags/dbt/jaffle-shop-classic",
                    "target":"dev"
        }


    @task.bash(env={"DBT_USER":user,"DBT_ENV_SECRET_PASSWORD":password})
    def my_bash_task(**context):
        bash_script = f"""
        source /usr/local/airflow/dbt_venv/bin/activate && 
        dbt deps --profiles-dir {dbt_vars["profiles_dir"]} --project-dir {dbt_vars["project_dir"]} && 
        dbt build --profiles-dir {dbt_vars["profiles_dir"]} --project-dir {dbt_vars["project_dir"]}
        """
        return bash_script

    @task
    def print_logs(**context):
        with open(f'/usr/local/airflow/logs/dag_id=local_bash_operator_dbt_dag/run_id={context["dag_run"].run_id}/task_id=my_bash_task/attempt=1.log', 'r') as file:
            # Read and print each line
            for line in file:
                print(line, end='')

    my_bash_task() >> print_logs()
jaffle_shop()
