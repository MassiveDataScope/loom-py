"""Who emits the records: the service and the kind of process."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from loom.core.environment import Environment
from loom.core.log_names import LogField


class LogRuntime(StrEnum):
    """The kind of process that emits the records."""

    FASTAPI = "fastapi"
    CELERY = "celery"
    LAMBDA = "lambda"
    CLI = "cli"


@dataclass(frozen=True, slots=True)
class LogIdentity:
    """Which service, and which process of it, emits the records.

    The runtime is known only once an entry point composes the application, after the
    configuration that builds this identity has loaded. :meth:`run_as` is therefore the
    one mutation, and every output holds this same instance, so it changes what the
    following records say on all of them; every other field is read-only.

    Attributes:
        environment: The environment the process runs in.
        runtime: The kind of process.
        service_name: ``app.name``; absent for a process with no application.
        service_version: ``app.version`` when declared.
    """

    environment: Environment
    runtime: LogRuntime = LogRuntime.CLI
    service_name: str | None = None
    service_version: str | None = None
    _fields: Mapping[str, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_fields", self._known())

    def run_as(self, runtime: LogRuntime) -> None:
        """Name *runtime* as the kind of process from now on."""
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "_fields", self._known())

    def fields(self) -> Mapping[str, str]:
        """The fields it writes, without those it does not know."""
        return self._fields

    def _known(self) -> Mapping[str, str]:
        known = {
            LogField.SERVICE_NAME: self.service_name,
            LogField.SERVICE_VERSION: self.service_version,
            LogField.ENVIRONMENT: self.environment.value,
            LogField.RUNTIME: self.runtime.value,
        }
        return MappingProxyType({key: value for key, value in known.items() if value is not None})
