#!/usr/bin/env bash
set -euo pipefail

current_package="loom/stores/sqlalchemy/rls"
released_packages=("loom/core/repository/sqlalchemy/rls" "$current_package")
compared=0
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

usage() {
  echo "usage: $0 tags | wheel <published.whl> <candidate.whl>" >&2
  exit 2
}

released_package() {
  local root="$1" candidate
  for candidate in "${released_packages[@]}"; do
    if [ -d "$root/$candidate/guard" ]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

compare() {
  local released_root="$1" current_root="$2" label="$3" released file name digest
  released="$(released_package "$released_root")" || return 0
  for file in "$released_root/$released/guard/"*.sql; do
    [ -e "$file" ] || continue
    name="${file##*/}"
    if ! cmp -s "$file" "$current_root/$current_package/guard/$name"; then
      echo "::error file=src/$current_package/guard/$name::$name was released in $label and must stay byte-identical; add a new revision"
      return 1
    fi
    compared=$((compared + 1))
  done
  for digest in $(grep -oE '[0-9a-f]{64}' "$released_root/$released/guard_manifest.py" || true); do
    if ! grep -qF "$digest" "$current_root/$current_package/guard_manifest.py"; then
      echo "::error file=src/$current_package/guard_manifest.py::digest $digest released in $label is missing; the manifest only grows"
      return 1
    fi
  done
}

check_tags() {
  local tag tree candidate count=0
  for tag in $(git tag --list 'v*' --sort=v:refname); do
    tree="$work/$tag"
    mkdir -p "$tree"
    for candidate in "${released_packages[@]}"; do
      if git cat-file -e "$tag:src/$candidate/guard" 2>/dev/null; then
        git archive "$tag" "src/$candidate" | tar -x -C "$tree"
        compare "$tree/src" src "$tag"
        break
      fi
    done
    count=$((count + 1))
  done
  echo "released guard revisions unchanged across $count release tags ($compared files compared)"
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
