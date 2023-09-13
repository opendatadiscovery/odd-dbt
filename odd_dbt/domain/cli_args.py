from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dbt.cli.resolvers import default_project_dir, default_profiles_dir


@dataclass
class CliArgs:
    project_dir: Path
    profiles_dir: Path
    profile: Optional[str]
    target: Optional[str]
    threads: Optional[int]
    vars: Optional[dict[str, str]]

    @classmethod
    def default(cls):
        return cls(
            project_dir=default_project_dir(),
            profiles_dir=default_profiles_dir(),
            profile=None,
            target=None,
            threads=1,
            vars=dict(),
        )


@dataclass
class FlagsArgs:
    project_dir: Path
    profiles_dir: Path
    profile: Optional[str] = None
    target: Optional[str] = None
    vars: Optional[dict[str, str]] = None
    use_colors: Optional[bool] = True
