"""In-process cover for the on-demand OTLP exporter loaders.

The loaders import inside the function body, so their failure branches are
reachable without a subprocess: blocking the exporter module on ``meta_path``
is enough.  The subprocess tests in ``test_optional_otel_sdk`` exercise the
same branches end to end, but coverage cannot see into a subprocess, so the
guards would read as untested.
"""

from __future__ import annotations

import importlib
import importlib.abc
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pytest

from loom.core.config.observability import OtelConfig
from loom.core.observability.observer import otel

_GRPC = "opentelemetry.exporter.otlp.proto.grpc"
_HTTP = "opentelemetry.exporter.otlp.proto.http"


class _Blocker(importlib.abc.MetaPathFinder):
    """Refuse the named module prefixes, as an absent install would."""

    def __init__(self, prefixes: Sequence[str]) -> None:
        self._prefixes = tuple(prefixes)

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        if any(fullname == p or fullname.startswith(f"{p}.") for p in self._prefixes):
            raise ImportError(f"blocked by test: {fullname}")
        return None


@contextmanager
def _without(*prefixes: str) -> Iterator[None]:
    blocker = _Blocker(prefixes)
    evicted = {n: m for n, m in sys.modules.items() if n.startswith(prefixes)}
    for name in evicted:
        del sys.modules[name]
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        sys.meta_path.remove(blocker)
        sys.modules.update(evicted)


class TestSpanExporterLoader:
    def test_names_the_grpc_package_when_grpc_is_absent(self) -> None:
        with _without(_GRPC), pytest.raises(ValueError) as excinfo:
            otel._load_span_exporter_cls("grpc")
        assert "grpc" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_names_the_http_package_when_http_is_absent(self) -> None:
        with _without(_HTTP), pytest.raises(ValueError) as excinfo:
            otel._load_span_exporter_cls("http/protobuf")
        assert "http" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ImportError)

    @pytest.mark.parametrize("protocol", ["grpc", "http/protobuf"])
    def test_returns_the_exporter_class_when_installed(self, protocol: str) -> None:
        assert isinstance(otel._load_span_exporter_cls(protocol), type)


class TestLogExporterLoader:
    def test_names_the_grpc_package_when_grpc_is_absent(self) -> None:
        with _without(_GRPC), pytest.raises(ValueError) as excinfo:
            otel._load_log_exporter_cls("grpc")
        assert "grpc" in str(excinfo.value)

    def test_names_the_http_package_when_http_is_absent(self) -> None:
        with _without(_HTTP), pytest.raises(ValueError) as excinfo:
            otel._load_log_exporter_cls("http/protobuf")
        assert "http" in str(excinfo.value)

    def test_rejects_an_unsupported_protocol_without_importing(self) -> None:
        with pytest.raises(ValueError):
            otel._load_log_exporter_cls("thrift")

    @pytest.mark.parametrize("protocol", ["grpc", "http/protobuf"])
    def test_returns_the_exporter_class_when_installed(self, protocol: str) -> None:
        assert isinstance(otel._load_log_exporter_cls(protocol), type)


class TestBuildTracerSdkGuard:
    """``_build_tracer`` resolves the exporter before the SDK, and says so."""

    def test_returns_the_ambient_tracer_without_an_endpoint(self) -> None:
        tracer, provider = otel.build_tracer(OtelConfig(endpoint=""))
        assert provider is None
        assert tracer is not None

    def test_names_the_missing_exporter_before_the_missing_sdk(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/v1/traces")
        with _without(_HTTP, "opentelemetry.sdk"), pytest.raises(ValueError) as excinfo:
            otel.build_tracer(config)
        assert "http" in str(excinfo.value)

    def test_names_the_missing_sdk_when_the_exporter_is_present(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/v1/traces")
        importlib.import_module(f"{_HTTP}.trace_exporter")
        with _without("opentelemetry.sdk"), pytest.raises(ImportError) as excinfo:
            otel.build_tracer(config)
        assert "opentelemetry-sdk" in str(excinfo.value)

    def test_builds_a_private_provider_when_both_are_present(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/v1/traces")
        tracer, provider = otel.build_tracer(config)
        assert provider is not None
        assert tracer is not None
        provider.shutdown()


class TestLogEndpointResolution:
    """The logs endpoint is derived from the traces one, or overridden."""

    def test_derives_the_logs_path_from_a_traces_endpoint(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/v1/traces")
        assert otel._resolve_log_endpoint(config) == "http://collector:4318/v1/logs"

    def test_keeps_an_endpoint_that_already_names_logs(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/v1/logs")
        assert otel._resolve_log_endpoint(config) == "http://collector:4318/v1/logs"

    def test_appends_the_logs_path_to_a_bare_endpoint(self) -> None:
        config = OtelConfig(endpoint="http://collector:4318/")
        assert otel._resolve_log_endpoint(config) == "http://collector:4318/v1/logs"

    def test_returns_none_without_an_endpoint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", raising=False)
        assert otel._resolve_log_endpoint(OtelConfig(endpoint="")) is None

    def test_prefers_the_logs_specific_environment_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://elsewhere:4318/v1/logs")
        config = OtelConfig(endpoint="http://collector:4318/v1/traces")
        assert otel._resolve_log_endpoint(config) == "http://elsewhere:4318/v1/logs"

    def test_prefers_the_logs_specific_protocol_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "grpc")
        assert otel._resolve_log_protocol(OtelConfig(endpoint="")) == "grpc"
