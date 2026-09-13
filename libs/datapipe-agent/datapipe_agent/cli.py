import click
import sys
import os

from datapipe.compute import DatapipeApp
from datapipe_agent import DatapipeAgent, AgentSettings


def register_commands(cli: click.Group):
    @cli.command()
    @click.option("--name", type=click.STRING, default="datapipe-agent")
    @click.option("--router_host", type=click.STRING, default=None)
    @click.option("--router_port", type=click.INT, default=None)
    @click.option("--storage_host", type=click.STRING, default=None)
    @click.option("--storage_port", type=click.INT, default=None)
    @click.option("--runner_type", type=click.STRING, default=None)
    @click.option("--namespace", type=click.STRING, default=None)
    @click.option("--image", type=click.STRING, default=None)
    @click.option("--tag", type=click.STRING, default=None)
    @click.option("--job-prefix", type=click.STRING, default=None)

    @click.pass_context
    def agent(ctx: click.Context, *args, **kwargs) -> None:
        app: DatapipeApp = ctx.obj["pipeline"]
        defaults = {k: v for k,v in kwargs.items() if v is not None}
        settings: AgentSettings = AgentSettings.from_env(**defaults)

        agent: DatapipeAgent = DatapipeAgent(app, settings)

        import asyncio

        try:
            asyncio.run(agent.run_agent())
        except Exception as e:
            print(f"Daemon crashed due to unhandled error: {e}")
            sys.exit(1)
