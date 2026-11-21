AI API
======

.. note::

   The programmatic contracts below are **experimental** and may change within
   a major line (FR-056). The authored **artifact format is not** — a definition
   declaring ``spec_version: 1`` keeps validating and compiling for the whole
   major line (FR-056a). See :doc:`../../ai/overview`.

Pillar surface
--------------

.. autosummary::
   :toctree: generated

   loom.ai
   loom.ai.abc
   loom.ai.config
   loom.ai.inference
   loom.ai.errors
   loom.ai.execution

Authored artifacts
------------------

.. autosummary::
   :toctree: generated

   loom.ai.declarative
   loom.ai.validate

Compilation
-----------

.. autosummary::
   :toctree: generated

   loom.ai.compiler
   loom.ai.registry
   loom.ai.describe

Interoperability
----------------

.. autosummary::
   :toctree: generated

   loom.ai.a2a
   loom.ai.fastapi
