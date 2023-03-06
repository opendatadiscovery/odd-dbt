import subprocess
import traceback
from pathlib import Path

import typer
from odd_models.api_client.v2.odd_api_client import Client
from odd_models.logger import logger
from oddrn_generator import DbtGenerator

from odd_dbt.context import DbtContext
from odd_dbt.mapper.dbt_test import DbtTestMapper

app = typer.Typer(short_help="Run dbt tests and inject results to ODD platform")


@app.command()
def test(
    project_dir: Path = typer.Option(default=Path().cwd()),
    target: str = typer.Option(None, "--target", "-t"),
    profile_name: str = typer.Option(None, "--profile", "-p"),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
    dbt_host: str = typer.Option("localhost", "--dbt-host"),
):
    try:
        logger.debug(f"Run dbt process with {project_dir=}, {target=}, {profile_name=}")
        process = subprocess.run(["dbt", "test", "--project-dir", project_dir])

        if process.returncode >= 2:
            logger.error("dbt test command failed")
            raise typer.Exit(2)

        client = Client(host=platform_host, token=platform_token)
        generator = DbtGenerator(host_settings=dbt_host)
        client.create_data_source(
            data_source_name="dbt", data_source_oddrn=generator.get_data_source_oddrn()
        )

        context = DbtContext(
            project_dir=project_dir, profile_name=profile_name, target=target
        )

        logger.success("Got DBT test context. Start mapping...")
        data_entities = DbtTestMapper(context=context, generator=generator).map()
        logger.success("Mapping finished. Start injecting...")
        client.ingest_data_entity_list(data_entities=data_entities)
        logger.success("Injecting finished.")
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
