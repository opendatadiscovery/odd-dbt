class DbtInternalError(Exception):
    def __init__(self, original_message: str, *args: object) -> None:
        super().__init__(original_message, *args)


class CantParseReason(Exception):
    ...


class ProfilerRenderError(Exception):
    def __str__(self):
        return "Couldn't parse profile config"
