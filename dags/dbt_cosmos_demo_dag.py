from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path
import os
from datetime import datetime
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_db",
        profile_args={"schema": "JAFFLE_SHOP"},
    ),
)
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt" 
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) 
my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/jaffle-shop-classic",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    # normal dag parameters
    operator_args={
        'install_deps': True,
        'full_refresh': False
    },
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 2},
)

# basic_cosmos_dag = DbtDag( 
#     # dbt/cosmos-specific parameters 
#     project_config=ProjectConfig( 
#         DBT_ROOT_PATH / "jaffle_shop", 
#     ), 
#     profile_config=profile_config, 
#     operator_args={ 
#         "install_deps": True,  # install any necessary dependencies before running any dbt command 
#         "full_refresh": True,  # used only in dbt commands that support this flag 
#     }, 
#     # normal dag parameters 
#     schedule_interval=None, 
#     start_date=datetime(2023, 1, 1), 
#     catchup=False, 
#     dag_id="basic_cosmos_dag", 
#     default_args={"retries": 2}, 
# ) 