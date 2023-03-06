import json
from pathlib import Path
from typing import Optional

import yaml

from odd_dbt.errors import DbtInternalError


def load_yaml(file_path: Path) -> dict:
    with file_path.open() as file:
        return yaml.load(file, yaml.FullLoader)


def load_json(file_path: Path) -> dict:
    with file_path.open() as file:
        return json.load(file)


class DbtContext:
    def __init__(
        self,
        project_dir: Path,
        profile_name: Optional[str] = None,
        target: Optional[str] = None,
    ):
        try:
            project = load_yaml(project_dir / "dbt_project.yml")

            target_path = project.get("target-path")

            if target_path is None:
                raise ValueError("Target path was not set")

            target_path = project_dir / target_path
            self.manifest = load_json(target_path / "manifest.json")
            self.run_results = load_json(target_path / "run_results.json")

            self.catalog = None
            if (catalog := target_path / "catalog.json").is_file():
                self.catalog = load_json(catalog)

            self.profile = self.parse_profile(
                project=project,
                profile_name=profile_name,
                target=target,
                profiles_dir=Path(self.run_results["args"]["profiles_dir"]),
            )
            self.tests = list(self.run_results.get("results", []))
        except Exception as e:
            raise DbtInternalError(f"Failed to parse dbt context: {e}") from e

    def parse_profile(
        self,
        project: dict,
        profiles_dir: Path,
        profile_name: Optional[str],
        target: Optional[str],
    ) -> dict:
        profile_name = profile_name or project.get("profile", None)

        if not profile_name:
            raise KeyError(f"Profile was not found in {project}")

        profile_config = load_yaml(profiles_dir / "profiles.yml")
        profile = profile_config.get(profile_name)

        if not profile:
            raise ValueError("Profile was not set")

        if target := target or profile.get("target"):
            return profile["outputs"][target]
        else:
            raise ValueError("Target was not set")
