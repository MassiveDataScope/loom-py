"""``create_kernel`` hands the executor the ``ElevationSink`` bound once the modules ran."""

from __future__ import annotations

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.di.container import LoomContainer
from tests.unit.core.authz._elevation_doubles import FakeSink
from tests.unit.core.bootstrap.kernel.components import AcmeConfig, AcmePing


def _sink_of(kernel: KernelRuntime) -> object:
    return kernel.executor._elevation_sink  # noqa: SLF001


def test_without_a_container_the_sink_a_module_bound_is_the_executors() -> None:
    sink = FakeSink()

    def binds_sink(container: LoomContainer) -> None:
        container.register_instance(ElevationSink, sink)

    kernel = create_kernel(config=AcmeConfig(), use_cases=[AcmePing], modules=[binds_sink])

    assert _sink_of(kernel) is sink
    assert kernel.container.resolve(ElevationSink) is sink


def test_on_a_container_the_bound_sink_is_the_executors() -> None:
    sink = FakeSink()
    container = LoomContainer()
    container.register_instance(ElevationSink, sink)

    kernel = create_kernel(container=container, use_cases=[AcmePing])

    assert _sink_of(kernel) is sink
    assert container.resolve(ElevationSink) is sink


def test_without_any_sink_the_executor_has_none() -> None:
    kernel = create_kernel(container=LoomContainer(), use_cases=[AcmePing])

    assert _sink_of(kernel) is None
