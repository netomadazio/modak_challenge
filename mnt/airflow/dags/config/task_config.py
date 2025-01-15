"""
This python file contains all tasks used in the ETL-check-allowances DAG
(check_recurring_allowances_process.py).
"""

task_dict = {
    "init_process_authentication": {
        "depends_on": [],
        "module_path": "tasks.init_process_authentication",
        "function_name": "init_process_authentication",
    },
    "look_for_inconsistencies": {
        "depends_on": ["init_process_authentication"],
        "module_path": "tasks.look_for_inconsistencies",
        "function_name": "look_for_inconsistencies",
        "kwargs": {
            "allowance_events": "https://gist.githubusercontent.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4/raw/13ded757082f09740a0ca351f926b74c336206ab/allowance_events",
            "allowance_backend": "https://gist.githubusercontent.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4/raw/13ded757082f09740a0ca351f926b74c336206ab/allowance_backend_table",
            "payment_schedule_backend": "https://gist.githubusercontent.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4/raw/13ded757082f09740a0ca351f926b74c336206ab/payment_schedule_backend_table"
        },
    },
    "look_allowance_backend": {
        "depends_on": ["init_process_authentication"],
        "module_path": "tasks.look_allowance_backend",
        "function_name": "look_for_inconsistencies",
        "kwargs": {
            "allowance_backend": "https://gist.githubusercontent.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4/raw/13ded757082f09740a0ca351f926b74c336206ab/allowance_backend_table",
        },
    },
    "look_payment_schedule": {
        "depends_on": ["init_process_authentication"],
        "module_path": "tasks.look_payment_schedule",
        "function_name": "look_for_inconsistencies",
        "kwargs": {
            "payment_schedule_backend": "https://gist.githubusercontent.com/DaniModak/d0cdc441bc2cab2abdc5b37e45ca5cb4/raw/13ded757082f09740a0ca351f926b74c336206ab/payment_schedule_backend_table"
        },
    },
}