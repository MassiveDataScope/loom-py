"""ETL declarations read from YAML, the input of ``discover_and_deploy_etls(config=...)``.

A declaration file has one of two shapes: a per-ETL document (the YAML
``etl_flow`` reads plus ``pipeline`` and ``params_type`` dotted paths, named by
``etl:`` or the file stem) or an ``etls:`` mapping of name to such a body.
Dotted paths are imported and type-checked here, so every error surfaces
before any deployment is registered. This module never deploys.
"""

from __future__ import annotations

import glob
import importlib
import typing
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

import msgspec

from loom.core.config import ConfigError, expand_config_glob
from loom.core.environment import ETL_CONFIG_VARIABLE
from loom.etl import ETLPipeline
from loom.prefect._flow_yaml import read_yaml, resolve_config_uri
from loom.prefect._meta import DEFAULT_STORAGE_CONFIG_PATH
from loom.prefect.flow import FlowSettings, flow_attribute_name, flow_settings_from_mapping
from loom.prefect.flow._assemble import check_inherited_params


@dataclass(frozen=True)
class EtlDeclaration:
    """One ETL declared in YAML, resolved and type-checked.

    Attributes:
        name: Free-form ETL name (Prefect flow and deployment name verbatim).
        attribute: Entrypoint attribute derived from ``name``; a Python identifier.
        config_uri: Resolved local path or cloud URI of the declaring file.
        pipeline: Imported ``ETLPipeline`` subclass.
        params_type: Params struct, explicit or inferred from the pipeline binding.
        settings: Flow settings derived exactly as ``etl_flow`` derives them.
        storage_config_path: Storage YAML path read at flow-run time.
    """

    name: str
    attribute: str
    config_uri: str
    pipeline: type[ETLPipeline[Any]]
    params_type: type[msgspec.Struct]
    settings: FlowSettings
    storage_config_path: str


def read_declarations(config: str) -> tuple[EtlDeclaration, ...]:
    """Read and validate every ETL declared by the files ``config`` names.

    Args:
        config: Local path, cloud URI or glob of declaration files.

    Returns:
        Declarations in file order, then document order.

    Raises:
        ConfigError: When a file declares no ETL, a dotted path does not import
            or has the wrong type, a name yields no identifier, ``job_variables``
            or ``job_variables.env`` is not a mapping, a declaration sets
            ``LOOM_ETL_CONFIG`` under ``job_variables.env``, a trigger inherits
            a parameter the params type does not declare, or two declarations
            share a name or an attribute.
    """
    declarations: list[EtlDeclaration] = []
    for uri in expand_config_glob(resolve_config_uri(config)):
        declarations.extend(_declaration(name, body, uri) for name, body in _bodies_in_file(uri))
    _check_unique(declarations)
    return tuple(declarations)


def load_declaration(config_uri: str, attribute: str) -> EtlDeclaration:
    """Return the declaration in ``config_uri`` whose entrypoint attribute matches.

    Only the matching body is imported and validated; sibling declarations in
    the same file are left untouched.

    Args:
        config_uri: Declaration file (not a glob).
        attribute: Entrypoint attribute derived from the ETL name.

    Returns:
        The matching declaration.

    Raises:
        ConfigError: When ``config_uri`` is a glob, or when no declaration maps
            to ``attribute``; the message lists the known attributes.
    """
    if glob.has_magic(config_uri):
        raise ConfigError(f"{config_uri}: a glob cannot identify one ETL declaration file")
    bodies = list(_bodies_in_file(config_uri))
    for name, body in bodies:
        if flow_attribute_name(name) == attribute:
            return _declaration(name, body, config_uri)
    known = ", ".join(sorted(flow_attribute_name(name) for name, _ in bodies))
    raise ConfigError(
        f"{config_uri}: no ETL maps to entrypoint attribute {attribute!r}; known: {known}"
    )


def _bodies_in_file(uri: str) -> Iterator[tuple[str, Any]]:
    document = read_yaml(uri)
    if "etls" in document:
        entries = document["etls"]
        if not isinstance(entries, Mapping):
            raise ConfigError(f"{uri}: 'etls' must be a mapping of ETL name to declaration")
        yield from ((str(name), body) for name, body in entries.items())
        return
    if "pipeline" in document:
        yield str(document.get("etl") or PurePosixPath(uri).stem), document
        return
    raise ConfigError(
        f"{uri}: declares neither 'etls' nor 'pipeline'; ETL declarations live in "
        "their own directory or in one 'etls:' file"
    )


def _declaration(name: str, body: Any, uri: str) -> EtlDeclaration:
    if not isinstance(body, Mapping):
        raise ConfigError(f"{uri}: ETL {name!r} must be a mapping")
    pipeline = _import_pipeline(name, body.get("pipeline"), uri)
    params_type = _resolve_params_type(name, body.get("params_type"), pipeline, uri)
    try:
        settings = flow_settings_from_mapping(body)
        check_inherited_params(settings.trigger, params_type, name=name)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{uri}: ETL {name!r}: {exc}") from exc
    _reject_reserved_env(name, settings, uri)
    return EtlDeclaration(
        name=name,
        attribute=_attribute_for(name, uri),
        config_uri=uri,
        pipeline=pipeline,
        params_type=params_type,
        settings=settings,
        storage_config_path=str(body.get("storage_config_path", DEFAULT_STORAGE_CONFIG_PATH)),
    )


def _reject_reserved_env(name: str, settings: FlowSettings, uri: str) -> None:
    """Refuse a declaration that sets the variable the deployer records itself.

    Also validates that ``job_variables`` and ``job_variables.env``, when
    present, are mappings; ``_discovery._with_recorded_env`` assumes this and
    merges them as one.
    """
    for environment, pool in settings.pool_config.items():
        job_variables = pool.get("job_variables") or {}
        if not isinstance(job_variables, Mapping):
            raise ConfigError(
                f"{uri}: ETL {name!r}: environments.{environment}.job_variables must be "
                f"a mapping, not {type(job_variables).__name__}"
            )
        env = job_variables.get("env")
        if env is None:
            continue
        if not isinstance(env, Mapping):
            raise ConfigError(
                f"{uri}: ETL {name!r}: environments.{environment}.job_variables.env must be "
                f"a mapping of environment variable name to value, not {type(env).__name__}"
            )
        if ETL_CONFIG_VARIABLE in env:
            raise ConfigError(
                f"{uri}: ETL {name!r}: environments.{environment}.job_variables.env may not "
                f"set {ETL_CONFIG_VARIABLE}; the deployer records it from the validated declaration"
            )


def _attribute_for(name: str, uri: str) -> str:
    attribute = flow_attribute_name(name)
    if not attribute.isidentifier():
        raise ConfigError(
            f"{uri}: ETL {name!r} maps to entrypoint attribute {attribute!r}, "
            "which is not a Python identifier"
        )
    return attribute


def _import_pipeline(etl: str, dotted: Any, uri: str) -> type[ETLPipeline[Any]]:
    if not isinstance(dotted, str) or not dotted:
        raise ConfigError(f"{uri}: ETL {etl!r}: 'pipeline' (dotted path) is required")
    obj = _import_object(etl, dotted, uri)
    if not (isinstance(obj, type) and issubclass(obj, ETLPipeline)):
        raise ConfigError(f"{uri}: ETL {etl!r}: {dotted!r} is not an ETLPipeline subclass")
    return obj


def _resolve_params_type(
    etl: str, dotted: Any, pipeline: type[ETLPipeline[Any]], uri: str
) -> type[msgspec.Struct]:
    if dotted is not None:
        obj = _import_object(etl, str(dotted), uri)
        if not _is_struct_type(obj):
            raise ConfigError(f"{uri}: ETL {etl!r}: {dotted!r} is not a msgspec.Struct subclass")
        return obj
    inferred = _bound_params_type(pipeline)
    if inferred is None:
        raise ConfigError(
            f"{uri}: ETL {etl!r}: {pipeline.__qualname__} does not bind "
            "ETLPipeline[...] to a msgspec.Struct; set 'params_type'"
        )
    return inferred


def _bound_params_type(pipeline: type[ETLPipeline[Any]]) -> type[msgspec.Struct] | None:
    for klass in pipeline.__mro__:
        for base in vars(klass).get("__orig_bases__", ()):
            if typing.get_origin(base) is not ETLPipeline:
                continue
            args = typing.get_args(base)
            if args and _is_struct_type(args[0]):
                return args[0]
    return None


def _is_struct_type(obj: Any) -> typing.TypeGuard[type[msgspec.Struct]]:
    return isinstance(obj, type) and issubclass(obj, msgspec.Struct)


def _import_object(etl: str, dotted: str, uri: str) -> Any:
    module_name, _, attribute = dotted.rpartition(".")
    if not module_name or not attribute:
        raise ConfigError(f"{uri}: ETL {etl!r}: {dotted!r} is not a 'package.module.Name' path")
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ConfigError(
            f"{uri}: ETL {etl!r}: cannot import {dotted!r}: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ConfigError(
            f"{uri}: ETL {etl!r}: cannot import {dotted!r}: "
            f"module {module_name!r} has no attribute {attribute!r}"
        ) from exc


def _check_unique(declarations: list[EtlDeclaration]) -> None:
    by_name: dict[str, EtlDeclaration] = {}
    by_attribute: dict[str, EtlDeclaration] = {}
    for declaration in declarations:
        same_name = by_name.get(declaration.name)
        if same_name is not None:
            raise ConfigError(
                f"ETL {declaration.name!r} is declared in {same_name.config_uri!r} "
                f"and in {declaration.config_uri!r}"
            )
        same_attribute = by_attribute.get(declaration.attribute)
        if same_attribute is not None:
            raise ConfigError(
                f"ETLs {same_attribute.name!r} ({same_attribute.config_uri}) and "
                f"{declaration.name!r} ({declaration.config_uri}) both map to entrypoint "
                f"attribute {declaration.attribute!r}"
            )
        by_name[declaration.name] = declaration
        by_attribute[declaration.attribute] = declaration


__all__ = ["EtlDeclaration", "load_declaration", "read_declarations"]
