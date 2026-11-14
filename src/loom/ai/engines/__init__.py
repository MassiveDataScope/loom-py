"""Engine adapters of the AI pillar, one subpackage per engine.

Nothing is imported here: an adapter is reached only through its entry point
in group ``loom.ai.engines``, so a base installation without any engine extra
imports ``loom.ai`` — and this package — without pulling a vendor SDK.
"""

from __future__ import annotations
