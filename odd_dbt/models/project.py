from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field


class Project(BaseModel):
    name: str
    profile: Optional[str]
    models_paths: Optional[List[str]] = Field(alias="models-paths")
    seed_paths: Optional[List[str]] = Field(alias="seed-paths")
    test_paths: Optional[List[str]] = Field(alias="test-paths")
    analysis_paths: Optional[List[str]] = Field(alias="analysis-paths")
    macro_paths: Optional[List[str]] = Field(alias="macro-paths")
    snapshot_paths: Optional[List[str]] = Field(alias="snapshot-paths")
    docs_paths: Optional[List[str]] = Field(alias="docs-paths")
    asset_paths: Optional[List[str]] = Field(alias="asset-paths")
    target_path: Optional[Path] = Field(alias="target-path")
