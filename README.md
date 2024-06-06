# Project Overview

## Purpose

The primary objective of this project is to **demonstrate various methods to orchestrate dbt (data build tool) using the Apache Airflow**. This includes showcasing:

- Integration patterns between dbt and Airflow.
- Techniques to optimize the execution of dbt jobs within Airflow DAGs.
- ** This project utilizes the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) and Docker for running Airflow. If you are running a different flavor of Airflow, copy the content in the dags folder to your development environment and ensure you have the needed packages in your [requirements.txt](https://github.com/sfc-gh-evenlet/dbt_orchestration_demo/blob/main/requirements.txt)

## Why dbt with Airflow?

Leveraging the Airflow with dbt's data transformation capabilities offers:

- **Scalable**, **maintainable**, and **enhanced** data pipelines.
- Superior **monitoring**, **dependency tracking**, and **scheduling** of transformation jobs through Airflow.
- Enhanced **data quality** and **reliability** through dbt.

## What's Inside?

This repository contains:

- Sample [jaffle_shop](https://github.com/sfc-gh-evenlet/dbt_orchestration_demo/tree/main/dags/dbt/jaffle-shop-classic) dbt model
- [dags/dbt_cosmos_demo_dag.py](https://github.com/sfc-gh-evenlet/dbt_orchestration_demo/blob/main/dags/dbt_cosmos_demo_dag.py): dag demonstrating the usage of dbt Cosmos for orchestrating
- [dags/dbt_bash_demo_dag.py](https://github.com/sfc-gh-evenlet/dbt_orchestration_demo/blob/main/dags/dbt_bash_demo_dag.py): dag demonstrating the usage of the bash task for orchestrating dbt
- [dags/dbt_virtual_python_demo_dag.py](https://github.com/sfc-gh-evenlet/dbt_orchestration_demo/blob/main/dags/dbt_virtual_python_demo_dag.py): dag demonstrating the usage of the virtual python operator and DbtRunner to orchestrate dbt

## Getting Started with Orchestrating dbt using Airflow

To get started with orchestrating dbt using the Astronomer build of Airflow:

1. Clone the repository.
2. Install the required dependencies, including the Astronomer CLI (if you want to use Astronomer Airflow for testing).

We hope this project serves as a valuable resource for anyone looking to streamline their data operations with dbt and Airflow.
