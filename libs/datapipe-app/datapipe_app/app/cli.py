import click

from datapipe_app import DatapipeAPI


def register_commands(cli: click.Group):
    @cli.command()
    @click.option("--host", type=click.STRING, default="0.0.0.0")
    @click.option("--port", type=click.INT, default=8000)
    @click.pass_context
    def api(ctx: click.Context, host: str, port: int) -> None:
        parent = ctx.parent
        assert parent is not None
        pipeline = ctx.obj["pipeline"]
        pipeline_spec = parent.params.get("pipeline", "app")

        import uvicorn

        if isinstance(pipeline, DatapipeAPI):
            api_app = pipeline
        else:
            api_app = DatapipeAPI(app=pipeline, pipeline_spec=pipeline_spec)

        uvicorn.run(api_app, host=host, port=port)
