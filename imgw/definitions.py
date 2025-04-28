from dagster import AssetSelection, Definitions, define_asset_job
from dagster_dlt import DagsterDltResource

from imgw.defs.assets import imgw_historic_datalake, imgw_real_time_datalake

dlt_resource = DagsterDltResource()

historic_job = define_asset_job(name="historic_job", selection=AssetSelection.groups("imgw_historic"))
real_time_job = define_asset_job(name="real_time_job", selection=AssetSelection.groups("imgw_real_time"))

defs = Definitions(
    assets=[imgw_real_time_datalake, imgw_historic_datalake],
    jobs=[historic_job, real_time_job],
    resources={
        "dlt": dlt_resource,
    },
)
