import traceback
from pathlib import Path
from typing import Optional

import typer
from dbt.cli.params import default_profiles_dir, default_project_dir
from odd_models.api_client.v2.odd_api_client import Client
from oddrn_generator import DbtGenerator

from odd_dbt import errors
from odd_dbt import get_version
from odd_dbt.logger import logger
from odd_dbt.mapper.test_results import DbtTestMapper
from odd_dbt.service.dbt import run_tests, CliArgs, get_context

app = typer.Typer(
    short_help="Run dbt tests and inject results to ODD platform",
    pretty_exceptions_show_locals=False,
)


@app.command()
def test(
    project_dir: Path = typer.Option(default=default_project_dir()),
    profiles_dir: Path = typer.Option(default=default_profiles_dir()),
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
    logger.info(f"Used OpenDataDiscovery dbt version: {get_version()}")
    cli_args = CliArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        threads=1,
        vars={},
    )
    try:
        logger.info(
            f"Run dbt process with {project_dir=}, {profiles_dir=}, {target=}, {profile=}"
        )
        run_tests(cli_args=cli_args)
        context = get_context(cli_args=cli_args)

        client = Client(host=platform_host, token=platform_token)
        generator = DbtGenerator(host_settings=dbt_host)

        if not dbt_data_source_oddrn:
            dbt_data_source_oddrn = generator.get_data_source_oddrn()
            logger.info(f"Creating data source for dbt: ODDRN={dbt_data_source_oddrn}")
            client.create_data_source(
                data_source_name="dbt",
                data_source_oddrn=dbt_data_source_oddrn,
            )

        data_entities = DbtTestMapper(context=context, generator=generator).map()

        logger.info("Mapping finished. Start injecting...")
        client.ingest_data_entity_list(data_entities=data_entities)

        logger.info("Injecting finished.")
    except errors.DbtTestCommandError as e:
        logger.error(e)
        typer.Exit(2)
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
