loom-kernel documentation
=========================

.. raw:: html

   <div class="loom-hero">
     <img src="_static/logo-transparent.png" alt="loom-kernel logo" />
   </div>
   <p align="center" style="margin:0.5rem 0 1.5rem;">
     <a href="https://github.com/MassiveDataScope/loom-py/actions/workflows/ci-main.yml">
       <img src="https://img.shields.io/github/actions/workflow/status/MassiveDataScope/loom-py/ci-main.yml?branch=master&label=ci" alt="CI" />
     </a>
     &nbsp;
     <a href="https://github.com/MassiveDataScope/loom-py/actions/workflows/docs.yml">
       <img src="https://img.shields.io/github/actions/workflow/status/MassiveDataScope/loom-py/docs.yml?branch=master&label=docs" alt="Docs" />
     </a>
     &nbsp;
     <a href="https://sonarcloud.io/summary/new_code?id=the-reacher-data_loom-py">
       <img src="https://sonarcloud.io/api/project_badges/measure?project=the-reacher-data_loom-py&metric=alert_status" alt="Quality Gate" />
     </a>
     &nbsp;
     <a href="https://sonarcloud.io/summary/new_code?id=the-reacher-data_loom-py">
       <img src="https://sonarcloud.io/api/project_badges/measure?project=the-reacher-data_loom-py&metric=security_rating" alt="Security" />
     </a>
     &nbsp;
     <a href="https://app.codecov.io/gh/MassiveDataScope/loom-py/tree/master">
       <img src="https://codecov.io/gh/MassiveDataScope/loom-py/branch/master/graph/badge.svg" alt="Coverage" />
     </a>
     &nbsp;
     <a href="https://pypi.org/project/loom-kernel/">
       <img src="https://img.shields.io/pypi/v/loom-kernel" alt="PyPI" />
     </a>
     &nbsp;
     <img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python" />
   </p>

Framework-agnostic toolkit to build backend applications with typed use cases,
repository contracts, and transport adapters.

Official hosted documentation:
`loom-py.readthedocs.io <https://loom-py.readthedocs.io/en/latest/>`_.
Companion demo application:
`dummy-loom <https://github.com/the-reacher-data/dummy-loom>`_.

.. toctree::
   :maxdepth: 2
   :caption: Overview

   overview

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting-started/rest
   getting-started/etl
   getting-started/streaming

.. toctree::
   :maxdepth: 2
   :caption: REST

   rest/autocrud
   rest/use-case-dsl
   rest/identity
   rest/authorization
   rest/postgres-rls
   rest/celery
   rest/sql
   rest/persistence-backends
   rest/cache
   rest/examples
   rest/testing

.. toctree::
   :maxdepth: 2
   :caption: ETL

   etl/pipelines
   etl/testing
   etl/examples

.. toctree::
   :maxdepth: 2
   :caption: Streaming

   streaming/bytewax
   streaming/delivery
   streaming/tracing

.. toctree::
   :maxdepth: 2
   :caption: AI Agents

   ai/overview
   ai/artifacts
   ai/providers
   ai/a2a
   ai/mcp
   ai/observability

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/index
