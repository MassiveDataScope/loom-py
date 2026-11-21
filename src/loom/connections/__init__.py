"""Outbound adapters that build the SDK client of each technology.

One package per technology (:mod:`~loom.connections.aws`,
:mod:`~loom.connections.mongo`, :mod:`~loom.connections.clickhouse`). Each
holds the ``client`` module, the only one that imports the SDK, and the
``loader`` that loads it on demand and names the missing extra. The settings
and value types the clients are built from stay in :mod:`loom.core.connections`.
Importing a package of this one loads no SDK.
"""
