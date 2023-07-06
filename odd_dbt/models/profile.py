from typing import Optional


class Profile:
    def __init__(self, **config) -> None:
        del config["password"]
        self._config = config

    @classmethod
    def from_dict(cls, profile: dict, target: Optional[str] = None) -> "Profile":
        target = profile.get("target", target)
        if not target:
            raise ValueError("Target must be set")
        profile = profile.get("outputs", {}).get(target, {})
        return cls(**profile)

    @property
    def type(self) -> str:
        return self._config["type"]

    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def account(self) -> Optional[str]:
        return self._config.get("account")
