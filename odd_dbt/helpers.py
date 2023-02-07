from typing import Optional


def parse_single_arg(args: list[str], keys: list[str], default=None) -> Optional[str]:
    """
    In provided argument list, find first key that has value and return that value.
    Values can be passed either as one argument {key}={value}, or two: {key} {value}
    """
    for key in keys:
        for i, arg in enumerate(args):
            if arg == key and len(args) > i:
                return args[i+1]
            if arg.startswith(f"{key}="):
                return arg.split("=", 1)[1]
    return default
