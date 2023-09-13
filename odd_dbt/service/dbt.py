import subprocess

import dbt.events.functions as events_functions
from dbt import flags

from odd_dbt import errors
from odd_dbt.domain.cli_args import CliArgs, FlagsArgs
from odd_dbt.domain.context import DbtContext
from odd_dbt.logger import logger
from odd_dbt import domain


def collect_test_results(context: DbtContext) -> list[domain.Result]:
    return context.results


def run_tests(cli_args: CliArgs) -> None:
    logger.info("Start dbt test process.")
    args = [
        "dbt",
        "test",
    ]

    project_dir = cli_args.project_dir
    profiles_dir = cli_args.profiles_dir
    profile = cli_args.profile
    target = cli_args.target

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
        raise errors.DbtTestCommandError("Could not run dbt test command.")

    logger.success("dbt test completed")


def collect_flags(cli_args: CliArgs):
    flag_args = FlagsArgs(
        project_dir=cli_args.project_dir,
        profiles_dir=cli_args.profiles_dir,
        target=cli_args.target,
        profile=cli_args.profile,
    )
    flags.set_from_args(flag_args, None)
    events_functions.set_invocation_id()

    return flags.get_flags()


def get_context(cli_args: CliArgs) -> DbtContext:
    collect_flags(cli_args)
    return DbtContext(cli_args=cli_args)
