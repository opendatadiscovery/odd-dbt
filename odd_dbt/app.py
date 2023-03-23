import subprocess
import traceback
from pathlib import Path
from typing import Optional

import typer
from odd_models.api_client.v2.odd_api_client import Client
from odd_models.logger import logger
from oddrn_generator import DbtGenerator

from odd_dbt.context import DbtContext
from odd_dbt.mapper.dbt_test import DbtTestMapper

app = typer.Typer(
    short_help="Run dbt tests and inject results to ODD platform",
    pretty_exceptions_show_locals=False,
)


@app.command()
def test(
    project_dir: Path = typer.Option(default=Path().cwd()),
    target: Optional[str] = typer.Option(default=None),
    profile_name: Optional[str] = typer.Option(default=None),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
    dbt_host: str = typer.Option(default="localhost"),
):
    try:
        logger.info(f"Run dbt process with {project_dir=}, {target=}, {profile_name=}")
        process = subprocess.run(["dbt", "test", "--project-dir", project_dir])

        if process.returncode >= 2:
            logger.error("Dbt test command failed")
            raise typer.Exit(2)

        client = Client(host=platform_host, token=platform_token)
        generator = DbtGenerator(host_settings=dbt_host)
        client.create_data_source(
            data_source_name="dbt", data_source_oddrn=generator.get_data_source_oddrn()
        )

        context = DbtContext(project_dir=project_dir, profile_name=None, target=None)

        logger.info("Got DBT test context. Start mapping...")
        data_entities = DbtTestMapper(context=context, generator=generator).map()

        logger.info("Mapping finished. Start injecting...")
        client.ingest_data_entity_list(data_entities=data_entities)

        logger.info("Injecting finished.")
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
