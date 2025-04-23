from collections.abc import Generator
from typing import Optional

from dagster import AssetExecutionContext, AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.dlt_event_iterator import DltEventType
from dagster_dlt.translator import DagsterDltTranslator, DltResourceTranslatorData
from dlt.common.destination import Destination
from dlt.extract import DltResource

from imgw import get_datalake_pipeline, get_local_pipeline, imgw_historic


class CustomDltTranslator(DagsterDltTranslator):
    def _custom_asset_key_fn(self, resource: DltResource, destination: Optional[Destination]) -> AssetKey:
        """Defines asset key for a given dlt resource key and dataset name.

        Args:
            resource (DltResource): dlt resource

        Returns:
            AssetKey of Dagster asset derived from dlt resource

        """
        if destination:
            return AssetKey(f"dlt_{destination.configured_name}_{resource.source_name}_{resource.name}")
        else:
            return AssetKey(f"dlt_{resource.source_name}_{resource.name}")

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        """Defines the asset spec for a given dlt resource.

        This method can be overridden to provide custom asset key for a dlt resource.

        Args:
            data (DltResourceTranslatorData): The dlt data to pass to the translator,
                including the resource and the destination.

        Returns:
            The :py:class:`dagster.AssetSpec` for the given dlt resource

        """
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            key=self._custom_asset_key_fn(resource=data.resource, destination=data.destination)
        )


@dlt_assets(
    dlt_source=imgw_historic(),
    dlt_pipeline=get_local_pipeline(),
    name="imgw_historic_local",
    group_name="imgw_local",
    dagster_dlt_translator=CustomDltTranslator(),
)
def imgw_historic_local(context: AssetExecutionContext, dlt: DagsterDltResource) -> Generator[DltEventType]:
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=imgw_historic(),
    dlt_pipeline=get_datalake_pipeline(),
    name="imgw_historic_datalake",
    group_name="imgw",
    dagster_dlt_translator=CustomDltTranslator(),
)
def imgw_historic_datalake(context: AssetExecutionContext, dlt: DagsterDltResource) -> Generator[DltEventType]:
    yield from dlt.run(context=context)
