import subprocess
import traceback
from pathlib import Path
from typing import Optional

import typer
from odd_models.api_client.v2.odd_api_client import Client
from oddrn_generator import DbtGenerator

from odd_dbt.context import DbtContext
from odd_dbt.logger import logger
from odd_dbt.mapper.dbt_test import DbtTestMapper

app = typer.Typer(
    short_help="Run dbt tests and inject results to ODD platform",
    pretty_exceptions_show_locals=False,
)


@app.command()
def test(
    project_dir: Path = typer.Option(default=Path.cwd()),
    profiles_dir: Path = typer.Option(default=None),
    target: Optional[str] = typer.Option(default=None),
    profile: Optional[str] = typer.Option(default=None),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
    dbt_data_source_oddrn: str = typer.Option(
        default=None,
    ),
    dbt_host: str = typer.Option(default="localhost"),
):
    logger.info("Start dbt test process. Version: 0.1.21")
    try:
        logger.info(
            f"Run dbt process with {project_dir=}, {profiles_dir=}, {target=}, {profile=}"
        )

        args = [
            "dbt",
            "test",
        ]

        if project_dir:
            args.extend(["--project-dir", project_dir])

        if profiles_dir:
            args.extend(["--profiles-dir", profiles_dir])

        if profile:
            args.extend(["--profile", profile])

        if target:
            args.extend(["--target", target])

        process = subprocess.run(args)

        if process.returncode >= 2:
            logger.error("Dbt test command failed")
            raise typer.Exit(2)

        client = Client(host=platform_host, token=platform_token)

        generator = DbtGenerator(host_settings=dbt_host)

        dbt_data_source_oddrn = dbt_data_source_oddrn
        if not dbt_data_source_oddrn:
            dbt_data_source_oddrn = generator.get_data_source_oddrn()
            logger.info(f"Creating data source for dbt: ODDRN={dbt_data_source_oddrn}")
            client.create_data_source(
                data_source_name="dbt",
                data_source_oddrn=dbt_data_source_oddrn,
            )

        context = DbtContext(project_dir=project_dir, profile=profile, target=target)

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
