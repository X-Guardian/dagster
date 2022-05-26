from dagster import AssetMaterialization, Field, In, Noneable, Nothing, Out, Output, op

from .resources import DEFAULT_POLL_INTERVAL, HightouchOutput


@op(
    required_resource_keys={"hightouch"},
    ins={"start_after": In(Nothing)},
    out=Out(
        HightouchOutput,
        description="Parsed json dictionary representing the details of the Hightouch "
        "sync after the sync successfully completes.",
    ),
    config_schema={
        "sync_id": Field(
            str, is_required=True, description="The Sync ID that this op will triger."
        ),
        "poll_interval": Field(
            float,
            default_value=DEFAULT_POLL_INTERVAL,
            description="The time (in seconds) that will be waited between successive "
            "polls.",
        ),
        "fail_on_warning": Field(
            bool,
            default_value=False,
            description="Whether to consider warnings a failure or success for an op.",
        ),
        "poll_timeout": Field(
            Noneable(float),
            default_value=None,
            description="The maximum time that will waited before this operation is "
            "timed out. By default, this will never time out.",
        ),
    },
    tags={"kind": "hightouch"},
)
def hightouch_sync_op(context):
    """
    Executes a Hightouch sync for a given ``sync_id``, and polls until that sync
    completes, raising an error if it is unsuccessful. It outputs a HightouchOutput
    which contains the details of the Hightouch connector after the sync run
    successfully completes.
    """

    hightouch_output: HightouchOutput = context.resources.hightouch.sync_and_poll(
        sync_id=context.op_config["sync_id"],
        fail_on_warning=context.op_config["fail_on_warning"],
        poll_interval=context.op_config["poll_interval"],
        poll_timeout=context.op_config["poll_timeout"],
    )
    context.log_event(
        AssetMaterialization(
            ["hightouch", "sync_run"],
            description="Hightouch Sync Run Details",
            metadata=dict(hightouch_output.sync_run_details),
        )
    )
    yield Output(hightouch_output)
