from typing import Any, Dict, Sequence, Type

import requests
from dagster import ConfigurableResource
from dagster._core.definitions.asset_spec import AssetSpec
from dagster._utils.cached_method import cached_method
from pydantic import Field

from dagster_powerbi.translator import (
    DagsterPowerBITranslator,
    PowerBIContentData,
    PowerBIContentType,
    PowerBIWorkspaceData,
)

BASE_API_URL = "https://api.powerbi.com/v1.0/myorg/"


class PowerBIWorkspace(ConfigurableResource):
    """Represents a workspace in PowerBI and provides utilities
    to interact with the PowerBI API.
    """

    api_token: str = Field(..., description="An API token used to connect to PowerBI.")
    workspace_id: str = Field(..., description="The ID of the PowerBI group to use.")

    def fetch_json(self, endpoint: str) -> Dict[str, Any]:
        """Fetch JSON data from the PowerBI API. Raises an exception if the request fails.

        Args:
            endpoint (str): The API endpoint to fetch data from.

        Returns:
            Dict[str, Any]: The JSON data returned from the API.
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_token}",
        }
        response = requests.get(
            f"{BASE_API_URL}/groups/{self.workspace_id}/{endpoint}", headers=headers
        )
        response.raise_for_status()
        return response.json()

    @cached_method
    def get_reports(self) -> Dict[str, Any]:
        """Fetches a list of all PowerBI reports in the workspace."""
        return self.fetch_json("reports")

    @cached_method
    def get_semantic_models(self) -> Dict[str, Any]:
        """Fetches a list of all PowerBI semantic models in the workspace."""
        return self.fetch_json("datasets")

    @cached_method
    def get_semantic_model_sources(
        self,
        dataset_id: str,
    ) -> Dict[str, Any]:
        """Fetches a list of all data sources for a given semantic model."""
        return self.fetch_json(f"datasets/{dataset_id}/datasources")

    @cached_method
    def get_dashboards(self) -> Dict[str, Any]:
        """Fetches a list of all PowerBI dashboards in the workspace."""
        return self.fetch_json("dashboards")

    @cached_method
    def get_dashboard_tiles(
        self,
        dashboard_id: str,
    ) -> Dict[str, Any]:
        """Fetches a list of all tiles for a given PowerBI dashboard,
        including which reports back each tile.
        """
        return self.fetch_json(f"dashboards/{dashboard_id}/tiles")

    def fetch_powerbi_workspace_data(
        self,
    ) -> PowerBIWorkspaceData:
        """Retrieves all Power BI content from the workspace and returns it as a PowerBIWorkspaceData object.
        Future work will cache this data to avoid repeated calls to the Power BI API.

        Returns:
            PowerBIWorkspaceData: A snapshot of the Power BI workspace's content.
        """
        dashboard_data = self.get_dashboards()["value"]
        augmented_dashboard_data = [
            {**dashboard, "tiles": self.get_dashboard_tiles(dashboard["id"])}
            for dashboard in dashboard_data
        ]
        dashboards = [
            PowerBIContentData(content_type=PowerBIContentType.DASHBOARD, properties=data)
            for data in augmented_dashboard_data
        ]

        reports = [
            PowerBIContentData(content_type=PowerBIContentType.REPORT, properties=data)
            for data in self.get_reports()["value"]
        ]
        semantic_models_data = self.get_semantic_models()["value"]
        data_sources_by_id = {}
        for dataset in semantic_models_data:
            dataset_sources = self.get_semantic_model_sources(dataset["id"])["value"]
            dataset["sources"] = [source["datasourceId"] for source in dataset_sources]
            for data_source in dataset_sources:
                data_sources_by_id[data_source["datasourceId"]] = PowerBIContentData(
                    content_type=PowerBIContentType.DATA_SOURCE, properties=data_source
                )
        semantic_models = [
            PowerBIContentData(content_type=PowerBIContentType.SEMANTIC_MODEL, properties=dataset)
            for dataset in semantic_models_data
        ]
        return PowerBIWorkspaceData(
            dashboards_by_id={dashboard.properties["id"]: dashboard for dashboard in dashboards},
            reports_by_id={report.properties["id"]: report for report in reports},
            semantic_models_by_id={
                dataset.properties["id"]: dataset for dataset in semantic_models
            },
            data_sources_by_id=data_sources_by_id,
        )

    def build_asset_specs(
        self,
        dagster_powerbi_translator: Type[DagsterPowerBITranslator] = DagsterPowerBITranslator,
    ) -> Sequence[AssetSpec]:
        """Fetches Power BI content from the workspace and translates it into AssetSpecs,
        using the provided translator.
        Future work will cache this data to avoid repeated calls to the Power BI API.

        Args:
            dagster_powerbi_translator (Type[DagsterPowerBITranslator]): The translator to use
                to convert Power BI content into AssetSpecs. Defaults to DagsterPowerBITranslator.

        Returns:
            Sequence[AssetSpec]: A list of AssetSpecs representing the Power BI content.
        """
        workspace_data = self.fetch_powerbi_workspace_data()
        translator = dagster_powerbi_translator(context=workspace_data)

        all_content = [
            *workspace_data.dashboards_by_id.values(),
            *workspace_data.reports_by_id.values(),
            *workspace_data.semantic_models_by_id.values(),
            *workspace_data.data_sources_by_id.values(),
        ]

        return [translator.get_asset_spec(content) for content in all_content]
