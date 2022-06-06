from typing import Any

from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.test.utils import execute_dagster_graphql, infer_pipeline_selector

from dagster.core.test_utils import wait_for_runs_to_finish

from .graphql_context_test_suite import GraphQLContextVariant, make_graphql_context_test_suite

RUN_QUERY = """
query RunQuery($runId: ID!) {
  pipelineRunOrError(runId: $runId) {
    __typename
    ... on Run {
      status
      stats {
        ... on RunStatsSnapshot {
          stepsSucceeded
        }
      }
    }
  }
}
"""


BaseTestSuite: Any = make_graphql_context_test_suite(
    context_variants=GraphQLContextVariant.all_executing_variants()
)


class TestBasicLaunch(BaseTestSuite):
    def test_run_launcher(self, graphql_context):
        selector = infer_pipeline_selector(graphql_context, "no_config_pipeline")
        result = execute_dagster_graphql(
            context=graphql_context,
            query=LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={"executionParams": {"selector": selector, "mode": "default"}},
        )

        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchRunSuccess"
        assert result.data["launchPipelineExecution"]["run"]["status"] == "STARTING"

        run_id = result.data["launchPipelineExecution"]["run"]["runId"]

        wait_for_runs_to_finish(graphql_context.instance)

        result = execute_dagster_graphql(
            context=graphql_context, query=RUN_QUERY, variables={"runId": run_id}
        )
        assert result.data["pipelineRunOrError"]["__typename"] == "Run"
        assert result.data["pipelineRunOrError"]["status"] == "SUCCESS"

    def test_run_launcher_subset(self, graphql_context):
        selector = infer_pipeline_selector(
            graphql_context, "more_complicated_config", ["noop_solid"]
        )
        result = execute_dagster_graphql(
            context=graphql_context,
            query=LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={
                "executionParams": {
                    "selector": selector,
                    "mode": "default",
                }
            },
        )

        assert result.data["launchPipelineExecution"]["__typename"] == "LaunchRunSuccess"
        assert result.data["launchPipelineExecution"]["run"]["status"] == "STARTING"

        run_id = result.data["launchPipelineExecution"]["run"]["runId"]

        wait_for_runs_to_finish(graphql_context.instance)

        result = execute_dagster_graphql(
            context=graphql_context, query=RUN_QUERY, variables={"runId": run_id}
        )
        assert result.data["pipelineRunOrError"]["__typename"] == "Run"
        assert result.data["pipelineRunOrError"]["status"] == "SUCCESS"
        assert result.data["pipelineRunOrError"]["stats"]["stepsSucceeded"] == 1
