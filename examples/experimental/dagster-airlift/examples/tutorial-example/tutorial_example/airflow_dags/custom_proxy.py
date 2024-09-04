import requests
from airflow.utils.context import Context
from dagster_airlift.in_airflow import BaseProxyToDagsterOperator


class CustomProxyToDagsterOperator(BaseProxyToDagsterOperator):
    def get_dagster_session(self, context: Context) -> requests.Session:
        api_key = context["var"]["value"].get("my_api_key")
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {api_key}"})
        return session

    def get_dagster_url(self, context: Context) -> str:
        return "https://dagster.example.com/"
