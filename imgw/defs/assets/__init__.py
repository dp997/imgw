from collections.abc import Generator

from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType

from imgw import get_datalake_pipeline, imgw_historic, imgw_real_time
from imgw.defs.assets.custom_functions import CustomDltTranslator as CustomDltTranslator


@dlt_assets(
    dlt_source=imgw_historic(),
    dlt_pipeline=get_datalake_pipeline(),
    name="imgw_historic_datalake",
    group_name="imgw_historic",
    dagster_dlt_translator=CustomDltTranslator(),
)
def imgw_historic_datalake(context: AssetExecutionContext, dlt: DagsterDltResource) -> Generator[DltEventType]:
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=imgw_real_time(),
    dlt_pipeline=get_datalake_pipeline(),
    name="imgw_real_time_datalake",
    group_name="imgw_real_time",
    dagster_dlt_translator=CustomDltTranslator(),
)
def imgw_real_time_datalake(context: AssetExecutionContext, dlt: DagsterDltResource) -> Generator[DltEventType]:
    yield from dlt.run(context=context)
