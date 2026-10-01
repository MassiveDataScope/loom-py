#!/usr/bin/env bash
set -euo pipefail

package="loom/core/repository/sqlalchemy/rls"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

usage() {
  echo "usage: $0 tags | wheel <published.whl> <candidate.whl>" >&2
  exit 2
}

compare() {
  local released="$1" current="$2" label="$3" file name digest
  [ -d "$released/$package/guard" ] || return 0
  for file in "$released/$package/guard/"*.sql; do
    [ -e "$file" ] || continue
    name="${file##*/}"
    if ! cmp -s "$file" "$current/$package/guard/$name"; then
      echo "::error file=src/$package/guard/$name::$name was released in $label and must stay byte-identical; add a new revision"
      return 1
    fi
  done
  for digest in $(grep -oE '[0-9a-f]{64}' "$released/$package/guard_manifest.py" || true); do
    if ! grep -qF "$digest" "$current/$package/guard_manifest.py"; then
      echo "::error file=src/$package/guard_manifest.py::digest $digest released in $label is missing; the manifest only grows"
      return 1
    fi
  done
}

check_tags() {
  local tag tree count=0
  for tag in $(git tag --list 'v*' --sort=v:refname); do
    tree="$work/$tag"
    mkdir -p "$tree"
    if git cat-file -e "$tag:src/$package/guard" 2>/dev/null; then
      git archive "$tag" "src/$package" | tar -x -C "$tree"
      compare "$tree/src" src "$tag"
    fi
    count=$((count + 1))
  done
  echo "released guard revisions unchanged across $count release tags"
}

unpack() {
  local wheel="$1" into="$2"
  [ -f "$wheel" ] || { echo "::error::$wheel not found" >&2; exit 1; }
  mkdir -p "$into"
  python3 -m zipfile -e "$wheel" "$into"
}

check_wheel() {
  unpack "$1" "$work/published"
  unpack "$2" "$work/candidate"
  compare "$work/published" "$work/candidate" "${1##*/}"
  echo "released guard revisions of ${1##*/} unchanged in ${2##*/}"
}

case "${1:-}" in
  tags) [ "$#" -eq 1 ] || usage; check_tags ;;
  wheel) [ "$#" -eq 3 ] || usage; check_wheel "$2" "$3" ;;
  *) usage ;;
esac
