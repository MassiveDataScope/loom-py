#!/usr/bin/env bash
set -euo pipefail
guard="src/loom/core/repository/sqlalchemy/rls/guard"
tag="$(git tag --list 'v*' --sort=-v:refname | head -n 1)"
if [ -z "$tag" ]; then
  echo "no release tag: nothing to compare"
  exit 0
fi
released="$(git ls-tree -r --name-only "$tag" -- "$guard" || true)"
for file in $released; do
  if ! git diff --quiet "$tag" -- "$file"; then
    echo "::error file=$file::$file was released in $tag and must not change; add a new revision"
    exit 1
  fi
done
echo "released guard revisions unchanged since $tag"
