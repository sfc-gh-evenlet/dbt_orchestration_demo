import json
import os
import pendulum
import subprocess
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

default_args = {
    'email': ['eric.venlet@snowflake.com'], 
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(
    dag_id="local_virtual_python_dbt_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["jaffle_shop","dbt"],
)
def jaffle_shop():

    # Retrieving connection detailsF
    connection = BaseHook.get_connection(conn_id='snowflake_db')
    password=connection.password
    user=connection.login

    @task.virtualenv(
        task_id="virtualenv_python_dbt_commands", 
        requirements=["dbt-snowflake==1.8.0"], 
        system_site_packages=False
    )
    def callable_virtualenv(user, password):
        import os
        import subprocess
        import logging
        from dbt.cli.main import dbtRunner, dbtRunnerResult
        
        os.environ['DBT_USER'] = user
        os.environ['DBT_ENV_SECRET_PASSWORD'] = password
        dbt_vars = {
                    "dbt_commands":['build'], 
                    "profiles_dir":"/usr/local/airflow/dags/.dbt", 
                    "project_dir":"/usr/local/airflow/dags/dbt/jaffle-shop",
                    "target":"dev"
        }
        dbt = dbtRunner()
        for command in dbt_vars["dbt_commands"]:    
            
            cli_args = [
                f'{command}',
                '--target',
                dbt_vars["target"],
                '--profiles-dir',
                dbt_vars["profiles_dir"],
                '--project-dir',
                dbt_vars["project_dir"]
            ]
            print(cli_args)
            res: dbtRunnerResult = dbt.invoke(cli_args)
            failed_model = False
            print(res.result)
            if command == "run" or command == "build":
                return_text = res.result
                for r in res.result:
                    if r.status == 'Fail':
                        failed_model = True
                    print(f"{r.node.name}: {r.status}")

            if failed_model:
                raise AirflowException("There was a failure in one of the models")

    run = callable_virtualenv(user, password)

    run 
jaffle_shop()