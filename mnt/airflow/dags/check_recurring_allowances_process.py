from datetime import datetime, timedelta
import importlib
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from config.task_config import task_dict

default_args = {
    "owner": "Irineu Madazio Neto",
    "start_date": datetime(2025, 1, 14),
    "depends_on_past": False,
    "email": ["netomadazio@hotmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def import_py_callable_and_kwargs(task):
    """
    Imports a module and function dynamically based on task settings.

    Args:
        task (dict): Dictionary containing task settings.

    Returns:
        function: Dynamically imported function.
    """
    def _made_function(**kwargs):
        try:
            module_path = task["module_path"]
            module = importlib.import_module(module_path)
            function_name = task["function_name"]
            function = getattr(module, function_name)
            if "kwargs" in task:
                task_kwargs = task["kwargs"]
                task_kwargs.update(kwargs)
                function(**task_kwargs)
            else:
                function(**kwargs)
        except ImportError as e:
            logging.error(f"Error importing module: {e}")
            raise
        except AttributeError as e:
            logging.error(f"Error accessing function: {e}")
            raise
    return _made_function
        

with DAG(
    "ETL-check-allowances",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["ETL", "allowances", "check"],
) as dag:
    
    for task_id, task_params in task_dict.items():

        py_callable = import_py_callable_and_kwargs(task_params)

        task_dict[task_id]["dag_instance"] = PythonOperator(
            task_id=task_id,
            python_callable=py_callable,
            op_kwargs=task_params.get("kwargs", {})
        )

        dependecies = task_params["depends_on"]

        for dependece_id in dependecies:
            (
                task_dict[task_id]["dag_instance"]
                << task_dict[dependece_id]["dag_instance"]
            )