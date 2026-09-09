import click
import sys
import os

from datapipe.compute import DatapipeApp
from datapipe_agent import DatapipeAgent


def register_commands(cli: click.Group):
    @cli.command()
    @click.option("--name", type=click.STRING, default="datapipe-agent")
    @click.option("--host", type=click.STRING, default="0.0.0.0")
    @click.option("--port", type=click.INT, default=8000)
    @click.pass_context
    def agent(ctx: click.Context, name:str,  host: str, port: int) -> None:
        app: DatapipeApp = ctx.obj["pipeline"]
        agent: DatapipeAgent = DatapipeAgent(
            app, 
            name=os.environ.get("DATAPIPE_AGENT_NAME", name), 
            server_host=os.environ.get("DATAPIPE_SERVER_HOST", host), 
            server_port=os.environ.get("DATAPIPE_SERVER_PORT", port)
        )

        import asyncio

        try:
            asyncio.run(agent.run_agent())
        except Exception as e:
            print(f"Daemon crashed due to unhandled error: {e}")
            sys.exit(1)
