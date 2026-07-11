#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

if [[ "${OSTYPE:-}" == linux* && -z "${BINDGEN_EXTRA_CLANG_ARGS:-}" ]]; then
  clang_resource_include=""
  if command -v clang >/dev/null 2>&1; then
    clang_resource_candidate="$(clang -print-resource-dir 2>/dev/null || true)/include"
    if [[ -f "$clang_resource_candidate/stddef.h" ]]; then
      clang_resource_include="$clang_resource_candidate"
    fi
  fi
  if [[ -z "$clang_resource_include" ]]; then
    for clang_resource_candidate in /usr/lib/llvm-*/lib/clang/*/include; do
      if [[ -f "$clang_resource_candidate/stddef.h" ]]; then
        clang_resource_include="$clang_resource_candidate"
      fi
    done
  fi
  if [[ -n "$clang_resource_include" ]]; then
    export BINDGEN_EXTRA_CLANG_ARGS="-isystem${clang_resource_include}"
  fi
fi

if [[ -z "${PROTOC:-}" ]]; then
  if ! command -v mise >/dev/null 2>&1; then
    echo "Missing mise. Install the version required by mise.toml, then run 'mise install --locked protoc'." >&2
    exit 1
  fi
  if ! PROTOC="$(cd "$repo_root" && mise which protoc)"; then
    echo "Pinned protoc is unavailable. Run 'mise install --locked protoc' from the repository root." >&2
    exit 1
  fi
  export PROTOC
fi

exec cargo "$@"
