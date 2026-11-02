"""Names of public worker manifest declarations."""

from enum import StrEnum


class WorkerManifestAttr(StrEnum):
    """Attribute names expected on a worker manifest module."""

    MANIFEST = "MANIFEST"
    MODELS = "MODELS"
    USE_CASES = "USE_CASES"
    INTERFACES = "INTERFACES"
    JOBS = "JOBS"
    CALLBACKS = "CALLBACKS"
    AGENTS = "AGENTS"
