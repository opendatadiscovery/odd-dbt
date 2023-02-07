import subprocess
import sys
import time
import os
from odd_dbt.helpers import parse_single_arg
from odd_dbt.parser import DbtArtifactParser
from odd_dbt.action import ODDAction
from odd_dbt.logger import logger

__version__ = "0.1.0"

logger.info(f"Running ODD dbt wrapper version {__version__}")
logger.info(f"Start main {sys.argv[1:]}")

args = sys.argv[1:]
target = parse_single_arg(args, ['-t', '--target'])
project_dir = parse_single_arg(args, ['--project-dir'], default='./')
profile_name = parse_single_arg(args, ['--profile'])
platform_host = parse_single_arg(args, ['--odd-platform-host'])
platform_token = parse_single_arg(args, ['--odd-platform-token'])

parser = DbtArtifactParser(
    target=target,
    project_dir=project_dir,
    profile_name=profile_name,
)

pre_run_time = time.time()

if len(sys.argv) >= 2 and sys.argv[1] in ['run', 'test', 'build']:
    try:
        # Execute dbt in external process
        process = subprocess.Popen(
            ["dbt"] + sys.argv[1:],
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        process.wait()
        if sys.argv[1] == 'test':
            executor = ODDAction(parser, platform_host, platform_token)
            executor.run()
    except Exception as e:
        logger.error(e)

# If run_result has modification time before dbt command
# or does not exist, do not generate ODD entities.
try:
    if os.stat(parser.run_result_path).st_mtime < pre_run_time:
        logger.info(f"ODD entities not generated: run_result file "
                    f"({parser.run_result_path}) was not modified by dbt")

except FileNotFoundError:
    logger.info(f"ODD entities not generated: "
                f"did not find run_result file ({parser.run_result_path})")
