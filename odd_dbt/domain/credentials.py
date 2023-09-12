class Credentials:
    def __init__(self, **config) -> None:
        del config["password"]
        self._config: dict[str, str] = config

    def __getitem__(self, item):
        return self._config.get(item)
