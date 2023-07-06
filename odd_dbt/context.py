from pathlib import Path
from typing import Optional

from odd_dbt.errors import DbtInternalError
from odd_dbt.models import Manifest, Profile, RunResults
from odd_dbt.models.project import Project
from odd_dbt.utils import load_json, load_yaml

from .logger import logger


class DbtContext:
    run_results: RunResults
    manifest: Manifest
    profile: Profile

    def __init__(
        self,
        project_dir: Path,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        profiles_dir: Optional[Path] = None,
    ):
        try:
            project = Project.parse_obj(load_yaml(project_dir / "dbt_project.yml"))

            if not (target_path := project.target_path):
                raise ValueError("Target path must be set")

            target_path = project_dir / target_path
            manifest = Manifest(target_path / "manifest.json")
            run_results = RunResults(target_path / "run_results.json")

            self.catalog = None
            if (catalog := target_path / "catalog.json").is_file():
                self.catalog = load_json(catalog)

            self.profile = self.parse_profile(
                project=project,
                profile=profile,
                target=target,
                profiles_dir=profiles_dir or run_results.profiles_dir,
            )
            self.run_results = run_results
            self.manifest = manifest
        except Exception as e:
            logger.debug(
                f"Was run with {profiles_dir=}, {profile=}, {target=}, {project_dir=}"
            )
            raise DbtInternalError(f"Failed to parse dbt context: {e}") from e

    def parse_profile(
        self,
        project: Project,
        profiles_dir: Path,
        profile: Optional[str],
        target: Optional[str],
    ) -> Profile:
        profile = profile or project.profile

        if not profile:
            raise ValueError("Profile was not found.")

        profiles = load_yaml(profiles_dir / "profiles.yml")

        if not (profile := profiles.get(profile)):
            raise ValueError("Profile was not set")

        return Profile.from_dict(profile, target)

    @property
    def results(self) -> list[dict]:
        return self.run_results.results

    @property
    def invocation_id(self) -> str:
        return self.run_results.invocation_id
