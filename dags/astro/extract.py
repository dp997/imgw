from airflow.decorators import dag
from dlt.helpers.airflow_helper import PipelineTasksGroup

default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


@dag(schedule=None, catchup=False, max_active_runs=1, default_args=default_task_args)
def load_imgw_data():
    tasks = PipelineTasksGroup("imgw_historic", use_data_folder=False, wipe_local_data=True, use_task_logger=True)

    from imgw.extract import get_dlt_datalake_pipeline, get_dlt_local_pipeline, imgw_historic, imgw_real_time

    tasks.add_run(
        get_dlt_datalake_pipeline(dataset_name="imgw_historic"),
        imgw_historic(),
        decompose="serialize",
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
    )


if __name__ == "__main__":
    load_imgw_data().test()
