from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class CliArgs:
    project_dir: Path
    profiles_dir: Path
    profile: Optional[str]
    target: Optional[str]
    threads: Optional[int]
    vars: Optional[dict[str, str]]


@dataclass
class FlagsArgs:
    project_dir: Path
    profiles_dir: Path
    profile: Optional[str] = None
    target: Optional[str] = None
    vars: Optional[dict[str, str]] = None
    use_colors: Optional[bool] = True
