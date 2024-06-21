from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field


class Project(BaseModel):
    name: str
    profile: Optional[str] = None
    models_paths: Optional[List[str]] = Field(None, alias="models-paths")
    seed_paths: Optional[List[str]] = Field(None, alias="seed-paths")
    test_paths: Optional[List[str]] = Field(None, alias="test-paths")
    analysis_paths: Optional[List[str]] = Field(None, alias="analysis-paths")
    macro_paths: Optional[List[str]] = Field(None, alias="macro-paths")
    snapshot_paths: Optional[List[str]] = Field(None, alias="snapshot-paths")
    docs_paths: Optional[List[str]] = Field(None, alias="docs-paths")
    asset_paths: Optional[List[str]] = Field(None, alias="asset-paths")
    target_path: Optional[Path] = Field(None, alias="target-path")
