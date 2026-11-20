"""MCP server over stdio, run as a child process by the stdio end-to-end test.

Writes its pid to the path given as the first argument so the test can assert
the process is gone once the host that spawned it has exited; the second
argument is the marker a tool result carries back.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from fastmcp import FastMCP

server: FastMCP = FastMCP("orders-stdio")


@server.tool
def read_orders(customer: str) -> str:
    """Return the orders of one customer."""
    return f"{customer}: 2 orders ({sys.argv[2]})"


@server.tool
def write_orders(customer: str, total: int) -> str:
    """Record an order for one customer."""
    return f"{customer}: recorded {total}"


@server.tool
def echo_env(name: str) -> str:
    """Return the value of one environment variable of this process."""
    return os.environ.get(name, "")


def main() -> None:
    """Publish the pid and serve over stdio until the parent closes stdin.

    Arguments: the file to write the pid to, and the marker the test looks for
    in a tool result.
    """
    Path(sys.argv[1]).write_text(str(os.getpid()), encoding="utf-8")
    server.run()


if __name__ == "__main__":
    main()
