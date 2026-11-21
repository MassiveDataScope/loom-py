"""HTTP surface of the AI pillar.

The only subpackage of :mod:`loom.ai` that imports FastAPI and Starlette, so
``import loom.ai`` keeps working on a base installation. Mounting mirrors
:func:`loom.rest.fastapi.sql.mount_sql_endpoints`: routes are added to an
existing application, and nothing here returns a router.
"""

from __future__ import annotations

from loom.ai.fastapi.endpoints import mount_agent_endpoints
from loom.ai.fastapi.response import AgentJSONResponse
from loom.ai.fastapi.streaming import HEARTBEAT_FRAME, encode_sse_event, stream_sse

__all__ = [
    "HEARTBEAT_FRAME",
    "AgentJSONResponse",
    "mount_agent_endpoints",
    "encode_sse_event",
    "stream_sse",
]
