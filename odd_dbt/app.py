import contextlib
import traceback
from pathlib import Path
from typing import Optional

import typer
from dbt.cli.params import default_profiles_dir, default_project_dir

from odd_dbt import config
from odd_dbt import errors
from odd_dbt import get_version
from odd_dbt.logger import logger
from odd_dbt.mapper.test_results import DbtTestMapper
from odd_dbt.mapper.lineage import DbtLineageMapper
from odd_dbt.libs import odd, dbt
from odd_dbt.service import odd as odd_api
from odd_dbt.service.dbt import run_tests, CliArgs

app = typer.Typer(
    short_help="Run dbt tests and inject results to ODD platform",
    pretty_exceptions_show_locals=False,
)


@contextlib.contextmanager
def handle_errors():
    try:
        yield
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
        raise typer.Exit(1)


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
        ..., "--dbt-oddrn", "-oddrn", envvar="DBT_DATA_SOURCE_ODDRN"
    ),
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
        run_tests(cli_args=cli_args)
        context = dbt.get_context(cli_args=cli_args)
        client = config.create_odd_client(host=platform_host, token=platform_token)
        generator = odd.create_dbt_generator_from_oddrn(oddrn=dbt_data_source_oddrn)
        data_entities = DbtTestMapper(context=context, generator=generator).map()

        odd_api.ingest_entities(data_entities, client)
    except errors.DbtTestCommandError as e:
        logger.error(e)
        typer.Exit(2)
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
        raise typer.Exit(1)


@app.command()
def create_datasource(
    data_source_name=typer.Option(..., "--name", "-n"),
    dbt_host: str = typer.Option(default="localhost"),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
):
    with handle_errors():
        client = config.create_odd_client(host=platform_host, token=platform_token)
        oddrn = odd_api.create_datasource(data_source_name, dbt_host, client)

        logger.info(f"Data source oddrn: '{oddrn}'")
        logger.info(
            "You can use command below to add newly created ODDRN for next commands:"
        )
        logger.info(f"export DBT_DATA_SOURCE_ODDRN={oddrn}")


@app.command()
def ingest_test(
    project_dir: Path = typer.Option(default=default_project_dir()),
    profiles_dir: Path = typer.Option(default=default_profiles_dir()),
    target: Optional[str] = typer.Option(default=None),
    profile: Optional[str] = typer.Option(default=None),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
    dbt_data_source_oddrn: str = typer.Option(
        ..., "--dbt-oddrn", "-oddrn", envvar="DBT_DATA_SOURCE_ODDRN"
    ),
):
    with handle_errors():
        cli_args = CliArgs(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile=profile,
            target=target,
            threads=1,
            vars={},
        )
        context = dbt.get_context(cli_args=cli_args)
        client = config.create_odd_client(host=platform_host, token=platform_token)
        generator = odd.create_dbt_generator_from_oddrn(oddrn=dbt_data_source_oddrn)

        data_entities = DbtTestMapper(context=context, generator=generator).map()
        odd_api.ingest_entities(data_entities, client)


@app.command()
def ingest_lineage(
    project_dir: Path = typer.Option(default=default_project_dir()),
    profiles_dir: Path = typer.Option(default=default_profiles_dir()),
    target: Optional[str] = typer.Option(default=None),
    profile: Optional[str] = typer.Option(default=None),
    platform_host: str = typer.Option(..., "--host", "-h", envvar="ODD_PLATFORM_HOST"),
    platform_token: str = typer.Option(
        ..., "--token", "-t", envvar="ODD_PLATFORM_TOKEN"
    ),
    dbt_data_source_oddrn: str = typer.Option(
        ..., "--dbt-oddrn", "-oddrn", envvar="DBT_DATA_SOURCE_ODDRN"
    ),
):
    with handle_errors():
        cli_args = CliArgs(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile=profile,
            target=target,
            threads=1,
            vars={},
        )
        context = dbt.get_context(cli_args=cli_args)
        client = config.create_odd_client(host=platform_host, token=platform_token)
        generator = odd.create_dbt_generator_from_oddrn(oddrn=dbt_data_source_oddrn)

        data_entities = DbtLineageMapper(context=context, generator=generator).map()
        odd_api.ingest_entities(data_entities, client)


if __name__ == "__main__":
    app()
