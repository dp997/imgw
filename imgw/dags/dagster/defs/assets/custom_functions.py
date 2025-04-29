from typing import Optional

from dagster import AssetKey, AssetSpec
from dagster_dlt.translator import DagsterDltTranslator, DltResourceTranslatorData
from dlt.common.destination import Destination
from dlt.extract import DltResource


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
