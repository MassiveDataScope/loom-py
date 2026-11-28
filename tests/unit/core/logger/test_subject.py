from __future__ import annotations

import subprocess
import sys

import pytest

from loom.core.environment import Environment
from loom.core.logger import subject as log_subject
from loom.core.logger.subject import LoggerEvent, SubjectHasher
from loom.core.model.redaction import Secret
from loom.core.observability.config import LogObservabilityConfig, ObservabilityConfig
from loom.core.observability.observability import Observability
from loom.testing import make
from tests.helpers.loggers import SUBJECT_KEY, CapturedLog, install_subject_key, user_hash


class TestSubjectHasher:
    @pytest.mark.parametrize(
        ("secret", "subject", "fields"),
        [
            (None, "alice", {}),
            (Secret(SUBJECT_KEY), "", {}),
            (Secret(SUBJECT_KEY), "José", {"user.hash": user_hash("José")}),
            (Secret(SUBJECT_KEY), "\udcff", {"user.hash": user_hash("\udcff")}),
        ],
        ids=["no-secret", "anonymous", "utf-8", "lone-surrogate"],
    )
    def test_a_subject_is_its_keyed_utf8_hash_and_nothing_without_a_secret_or_a_subject(
        self, secret: Secret[str] | None, subject: str, fields: dict[str, str]
    ) -> None:
        assert make(SubjectHasher, secret=secret).fields(subject) == fields


class TestTheSecretAtStartup:
    def test_the_configured_secret_is_installed_even_with_logging_disabled(self) -> None:
        log = make(LogObservabilityConfig, enabled=False, subject_key=Secret(SUBJECT_KEY))

        Observability.from_config(make(ObservabilityConfig, log=log), environment=Environment.TEST)

        assert SubjectHasher.installed().secret == Secret(SUBJECT_KEY)

    def test_a_process_built_twice_without_a_secret_after_one_with_it_warns_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        install_subject_key()

        Observability.from_config(ObservabilityConfig(), environment=Environment.TEST)
        Observability.from_config(ObservabilityConfig(), environment=Environment.TEST)

        warnings = CapturedLog(caplog, log_subject.__name__).messages
        assert warnings == [LoggerEvent.SUBJECT_HASH_DISABLED]


class TestAFreshProcess:
    _BUILD_TWICE = """
from loom.core.environment import Environment
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.observability import Observability

Observability.from_config(ObservabilityConfig(), environment=Environment.TEST)
Observability.from_config(ObservabilityConfig(), environment=Environment.TEST)
"""

    def test_built_twice_without_a_secret_warns_once(self) -> None:
        run = subprocess.run(
            [sys.executable, "-c", self._BUILD_TWICE], capture_output=True, text=True, check=True
        )

        assert run.stdout.count(LoggerEvent.SUBJECT_HASH_DISABLED) == 1
