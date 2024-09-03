import os
from typing import Callable, Type

import requests
from airflow.utils.context import Context

from dagster_airlift.in_airflow import BaseProxyToDagsterOperator


def default_retrieve_dagster_cloud_user_token(_: Context) -> str:
    dagster_cloud_user_token = os.environ.get("DAGSTER_CLOUD_USER_TOKEN")
    if not dagster_cloud_user_token:
        raise ValueError("DAGSTER_CLOUD_USER_TOKEN environment variable not set")
    return dagster_cloud_user_token


def build_cloud_proxy_operator(
    organization_name: str,
    deployment_name: str,
    user_token_retrieval_fn: Callable[[Context], str] = default_retrieve_dagster_cloud_user_token,
) -> Type[BaseProxyToDagsterOperator]:
    """Creates an operator type for proxying tasks to assets in a Dagster Cloud deployment.

    By default, this operator retrieves the Dagster Cloud user token from the DAGSTER_CLOUD_USER_TOKEN environment variable.
    This implementation can be overridden by passing a custom user_token_retrieval_fn to the constructor.

    Example:

    .. code-block:: python

            from dagster_airlift.in_airflow import build_cloud_proxy_operator, mark_as_dagster_migrating

            # ... dag code here

            MyCloudProxyOperator = build_cloud_proxy_operator("my-org", "my-deployment")
            mark_as_dagster_migrating(global_vars=globals(), dagster_operator_klass=MyCloudProxyOperator)

    Args:
        organization_name (str): The name of the organization that owns the deployment.
        deployment_name (str): The name of the deployment.
        user_token_retrieval_fn (Callable[[Context], str], optional): A function that retrieves the Dagster Cloud user token. Defaults
            to a function which retrieves the token from the DAGSTER_CLOUD_USER_TOKEN environment variable.
    """

    class DagsterCloudProxyOperator(BaseProxyToDagsterOperator):
        f"""Operator that proxies a task to assets in Dagster Cloud organization `{organization_name}` and deployment `{deployment_name}`.
        """

        def get_dagster_session(self, context: Context) -> requests.Session:
            dagster_cloud_user_token = user_token_retrieval_fn(context)
            session = requests.Session()
            session.headers.update({"Dagster-Cloud-Api-Token": dagster_cloud_user_token})
            return session

        def get_dagster_url(self) -> str:
            return f"https://{organization_name}.dagster.cloud/{deployment_name}"

    return DagsterCloudProxyOperator
