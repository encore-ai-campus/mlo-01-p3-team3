"""Stable error codes; SDK exception messages may contain credentials."""


class PipelineError(Exception):
    def __init__(self, code, detail=""):
        self.code = code
        super().__init__(f"{code}: {detail}" if detail else code)


class Conflict(PipelineError):
    pass


class LeaseLost(PipelineError):
    def __init__(self):
        super().__init__("LEASE_LOST")
