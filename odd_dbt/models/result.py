from funcy import first


class Result:
    def __init__(self, result: dict) -> None:
        self._result = result

    @property
    def unique_id(self) -> str:
        return self._result["unique_id"]

    @property
    def execution_period(self) -> (str, str):
        execution = first(
            period for period in self._result["timing"] if period["name"] == "execute"
        )

        if execution is None:
            return None, None

        return execution.get("started_at"), execution.get("completed_at")

    @property
    def status(self) -> str:
        return self._result["status"]
