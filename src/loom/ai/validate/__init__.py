"""Standalone agent artifact validation.

``python -m loom.ai.validate <globs>`` decodes every matching artifact and
runs the configuration-independent compiler phases — structure (via the
version envelope), output and policy limits — so a generator gets coded
feedback with no deployment configuration, no credentials and no network.

Exit status is ``0`` with an empty stderr when every artifact is valid;
otherwise every issue is printed to stderr as one line carrying its
:class:`~loom.ai.errors.AgentErrorCode`.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from glob import glob
from pathlib import Path

from loom.ai.compiler.phases import compile_output, validate_policies
from loom.ai.declarative import load_specs
from loom.ai.errors import AgentCompilationError, AgentCompilationIssue

__all__ = ["main", "validate_patterns"]


def validate_patterns(patterns: Sequence[str]) -> tuple[AgentCompilationIssue, ...]:
    """Validate every artifact matching the given glob patterns.

    Args:
        patterns: Glob patterns; absolute or relative to the working
            directory, expanded with ``**`` support.

    Returns:
        Every issue found across every matched artifact, in path order.
    """
    issues: list[AgentCompilationIssue] = []
    for path in _matched_files(patterns):
        issues.extend(_issues_for_file(path))
    return tuple(issues)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point of ``python -m loom.ai.validate``.

    Args:
        argv: Glob patterns; defaults to ``sys.argv[1:]``.

    Returns:
        ``0`` when every matched artifact is valid, ``1`` when issues were
        found, ``2`` on usage errors.
    """
    patterns = list(sys.argv[1:]) if argv is None else list(argv)
    if not patterns:
        print("usage: python -m loom.ai.validate <glob> [<glob> ...]", file=sys.stderr)
        return 2
    matched = _matched_files(patterns)
    if not matched:
        print(f"no artifact matched: {' '.join(patterns)}", file=sys.stderr)
        return 2
    issues = [issue for path in matched for issue in _issues_for_file(path)]
    for issue in issues:
        print(f"{issue.code.value} {issue.message}", file=sys.stderr)
    return 1 if issues else 0


def _matched_files(patterns: Sequence[str]) -> tuple[Path, ...]:
    """Resolve the globs to the files they name.

    Paths are taken as given. This is a local command line: the person who
    typed the pattern already has the process's own permissions, so there is
    no boundary here to escape and confining the result would only break the
    legitimate case of validating an artifact outside the working tree.

    Args:
        patterns: Glob patterns as given on the command line.

    Returns:
        The matched files, sorted and de-duplicated.
    """
    matches: set[Path] = set()
    for pattern in patterns:
        matches.update(Path(match) for match in glob(pattern, recursive=True))
    return tuple(sorted(path for path in matches if path.is_file()))


def _issues_for_file(path: Path) -> list[AgentCompilationIssue]:
    try:
        decoded = load_specs([path.name], root=path.parent)
    except AgentCompilationError as exc:
        return list(exc.issues)
    component = str(path)
    issues: list[AgentCompilationIssue] = []
    for item in decoded:
        issues.extend(item.issues)
        _, output_issues = compile_output(item.spec.output, component)
        issues.extend(output_issues)
        issues.extend(validate_policies(item.spec.policies, component))
    return issues
