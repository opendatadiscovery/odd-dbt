import json
import os
import yaml
from dataclasses import dataclass
from typing import Optional

from odd_dbt.logger import logger


@dataclass
class DbtRunContext:
    manifest: dict
    run_results: dict
    profile: dict
    catalog: dict = None


class DbtArtifactParser:
    def __init__(
            self,
            project_dir: str,
            profile_name: Optional[str] = None,
            target: Optional[str] = None,
    ):
        self.dir = os.path.abspath(project_dir)
        self.profile_name = profile_name
        self.target = target
        self.project = self._load_yaml(os.path.join(project_dir, 'dbt_project.yml'))
        self.manifest_path = os.path.join(self.dir, self.project['target-path'], 'manifest.json')
        self.run_result_path = os.path.join(
            self.dir, self.project['target-path'], 'run_results.json'
        )
        self.catalog_path = os.path.join(self.dir, self.project['target-path'], 'catalog.json')

    @staticmethod
    def _load_yaml(file_path: str) -> dict:
        with open(file_path) as file:
            res = yaml.load(file, yaml.FullLoader)
        return res

    @staticmethod
    def _load_json(file_path: str) -> dict:
        with open(file_path, 'r') as file:
            res = json.load(file)
        return res

    def _parse_profile(self, run_results: dict) -> dict:
        profile_dir = run_results['args']['profiles_dir']
        if not self.profile_name:
            if self.project.get('profile'):
                self.profile_name = self.project.get('profile')
            else:
                raise KeyError(f'profile not found in {self.project}')

        profile = self._load_yaml(os.path.join(profile_dir, 'profiles.yml'))[self.profile_name]

        if not self.target:
            self.target = profile['target']
        profile = profile['outputs'][self.target]

        return profile

    def parse_metadata(self) -> tuple[list[dict], DbtRunContext]:
        run_results = self._load_json(self.run_result_path)
        catalog = self._load_json(self.catalog_path) if os.path.isfile(self.catalog_path) else None
        context = DbtRunContext(
            manifest=self._load_json(self.manifest_path),
            run_results=run_results,
            profile=self._parse_profile(run_results),
            catalog=catalog,
        )
        tests = [node for node in run_results['results']]
        logger.info("Successfully parsed metadata files")

        return tests, context
