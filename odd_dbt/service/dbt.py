import subprocess

from odd_dbt import errors
from odd_dbt.domain.cli_args import CliArgs
from odd_dbt.logger import logger


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
