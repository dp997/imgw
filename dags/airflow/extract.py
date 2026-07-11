from airflow.sdk import dag
from dlt.helpers.airflow_helper import PipelineTasksGroup

default_task_args = {
    "owner": "airflow",
}


@dag(schedule=None, catchup=False, max_active_runs=1, default_args=default_task_args)
def load_imgw_historical():
    """
    Loads IMGW historical meteorological data into the datalake.
    Triggered manually.
    """
    tasks = PipelineTasksGroup("imgw_historic", use_data_folder=False, wipe_local_data=True, use_task_logger=True)

    from imgw.extract import get_dlt_datalake_pipeline, imgw_historic

    tasks.add_run(
        get_dlt_datalake_pipeline(dataset_name="imgw_historic"),
        imgw_historic(),
        decompose="serialize",
        retries=1,
    )


@dag(schedule=None, catchup=False, max_active_runs=1, default_args=default_task_args)
def load_imgw_real_time():
    """
    Loads IMGW real-time meteorological data into the datalake.
    Triggered manually.
    """
    tasks = PipelineTasksGroup("imgw_real_time", use_data_folder=False, wipe_local_data=True, use_task_logger=True)

    from imgw.extract import get_dlt_datalake_pipeline, imgw_real_time

    tasks.add_run(
        get_dlt_datalake_pipeline(dataset_name="imgw_real_time"),
        imgw_real_time(),
        decompose="serialize",
        retries=3,
    )


historical_dag = load_imgw_historical()
real_time_dag = load_imgw_real_time()
