#
# Ida Epic Caboodle Ingestion
# Description: Runs the ida epic caboodle ingestion into snowflake, does the testing, and logs to common_logging for data governance
# Author: Aaron Liske (aaron.liske@corewellhealth.org)
# Author2: Sean Brown (sean.brown@corewellhealth.org)
# Owner: Information Data Analytics - Data Engineering
#


import json
import os
import pendulum
import subprocess
import logging
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

doc_md_DAG = """
### This dag is set to run the Caboodle DBT Project Only. 
[CABOODLE BITBUCKET LINK](https://bitbucket.spectrum-health.org:7991/stash/projects/SNOW/repos/ida_epic_caboodle/browse)

"""

default_args = {
    'email': ['eric.venlet@snowflake.com'], #
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(
    dag_id="dbt_local_demo_dag",
    schedule="00 8 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["care","caboodle","epic"],
)
def snowflake_epic_caboodle():
    # Get all fields from variable
    dbt_vars = Variable.get("snowflake_ida_epic_caboodle", default_var=[], deserialize_json=True)

    # Retrieving connection detailsF
    connection = BaseHook.get_connection(conn_id=dbt_vars["airflow_connection_id"])
    extra = connection.extra_dejson
    password=connection.password
    user=connection.login
    sfk_schema = connection.schema
    sfk_database = extra.get('database')
    sfk_role = extra.get('role')
    sfk_wh = extra.get('warehouse')
    sfk_acct = extra.get('account')

    @task.virtualenv(
        task_id="virtualenv_python_dbt_commands", 
        requirements=["dbt-snowflake==1.7.0","pip-system-certs","GitPython"], 
        system_site_packages=False
    )
    def callable_virtualenv(user, password, sfk_schema, sfk_database, sfk_role, sfk_wh, sfk_acct, dbt_vars):
        import os
        import subprocess
        import logging
        from dbt.cli.main import dbtRunner, dbtRunnerResult
        from git import Repo
        
        os.environ['DBT_USER'] = user
        os.environ['DBT_ENV_SECRET_PASSWORD'] = password
        os.environ['SFK_SCHEMA'] = sfk_schema
        os.environ['SFK_DB'] = sfk_database
        os.environ['SFK_ROLE'] = sfk_role
        os.environ['SFK_WAREHOUSE'] = sfk_wh
        os.environ['SFK_ACCT'] = sfk_acct
        airflow_home='/usr/local/airflow'
        logger = logging.getLogger(__name__)
        
        def clone_repo(source, directory, branch="master"):
            if not os.path.exists(directory):
                repo = Repo.clone_from(source, directory,  branch=branch)
                print("Git working directory: ")
                print(repo.working_dir)
                print("Git branch:")
                print(repo.active_branch)
            else:
                repo = Repo(directory)
                print("Git working directory: ")
                print(repo.working_dir)
                print("Git branch:")
                print(repo.active_branch)
                repo.remotes[0].pull()
                
      
        def clone_dbt_project():
            print("Preparing to clone project")
            clone_repo(
                    dbt_vars["bitbucket_location"],
                    f"{airflow_home}/dbt/{dbt_vars['dbt_id']}",
                    branch=dbt_vars["branch"]
                    )
            print(os.path.exists(f"{airflow_home}/dbt/{dbt_vars['dbt_id']}"))
            clone_repo(
                    "https://bitbucket.spectrum-health.org:7991/stash/scm/cloud9/logging.git",
                    f"{airflow_home}/dbt/logging"
                    )
            print(os.path.exists(f"{airflow_home}/dbt/logging"))
        clone_dbt_project()
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
            res: dbtRunnerResult = dbt.invoke(cli_args)
            failed_model = False
            if command == "run" or command == "build":
                return_text = res.result
                for r in res.result:
                    if r.status == 'Fail':
                        failed_model = True
                    print(f"{r.node.name}: {r.status}")

            if failed_model:
                raise AirflowException("There was a failure in one of the models")

    run = callable_virtualenv(user, password, sfk_schema, sfk_database, sfk_role, sfk_wh, sfk_acct, dbt_vars)

    run 
