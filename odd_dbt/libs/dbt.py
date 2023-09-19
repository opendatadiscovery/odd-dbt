import dbt.events.functions as events_functions
from dbt import flags
from dbt.contracts.graph.nodes import ParsedNode, ModelNode, SeedNode

from odd_dbt.domain.cli_args import CliArgs, FlagsArgs
from odd_dbt.domain.context import DbtContext


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


def is_a_model_node(node: ParsedNode) -> bool:
    return isinstance(node, ModelNode)


def is_a_seed_node(node: SeedNode) -> bool:
    return isinstance(node, SeedNode)
