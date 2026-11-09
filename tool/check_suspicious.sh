#!/usr/bin/env bash
# Fails when a change adds something a websocket library has no reason to
# contain. A maintainer who has read the change can add the
# `security-reviewed` label to the pull request to let it through.
#
# Usage: tool/check_suspicious.sh <base-commit>
set -euo pipefail

base="${1:-}"
if [[ -z "$base" || "$base" =~ ^0+$ ]] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
  # A new branch: compare with where it left main. Without main, check the
  # whole tree.
  if git rev-parse -q --verify origin/main >/dev/null; then
    base=$(git merge-base origin/main HEAD)
  else
    base=$(git hash-object -t tree /dev/null)
  fi
fi

problems=()
flag() { problems+=("$1"); }

# Lines added by the change, outside docs and this checker itself.
added=$(git diff --unified=0 "$base" HEAD -- . \
  ':(exclude)*.md' ':(exclude)tool/check_suspicious.sh' \
  | grep -E '^\+[^+]' || true)

check() { # pattern, message
  if grep -qE "$1" <<<"$added"; then
    flag "$2"
    grep -nE "$1" <<<"$added" | head -5 | sed 's/^/      /' >&2
  fi
}

check "dart:ffi|DynamicLibrary" \
  "Native code (dart:ffi). A websocket library doesn't need it."
check "dart:mirrors|Isolate\.spawnUri" \
  "Loads or runs code dynamically."
check "Process\.(run|start|runSync)" \
  "Runs other programs."
check "Platform\.environment" \
  "Reads environment variables (could collect secrets)."
check "[A-Za-z0-9+/=]{200,}" \
  "A long encoded blob, which can hide a payload."
check "curl |wget |\| *(ba)?sh|base64 (-d|--decode)" \
  "Downloads or decodes and runs something."

# Files that run code on users' machines when the package is installed or
# built (Dart build hooks), or that add binaries.
changed=$(git diff --name-only --diff-filter=AM "$base" HEAD)
if grep -qE '^(hook|bin)/' <<<"$changed"; then
  flag "Adds a build hook or executable (hook/ or bin/), which runs on users' machines."
fi
binaries=$(git diff --numstat "$base" HEAD | awk '$1 == "-" { print $3 }')
if [[ -n "$binaries" ]]; then
  flag "Adds binary files: $(tr '\n' ' ' <<<"$binaries")"
fi

# New or changed dependencies need a human look.
if git diff "$base" HEAD -- pubspec.yaml \
  | grep -qE '^\+ +[a-z_]+:.*'; then
  if git diff "$base" HEAD -- pubspec.yaml | awk '
      /^[ +-]?(dependencies|dev_dependencies|dependency_overrides):/ { in_deps = 1; next }
      /^[ +-]?[a-z_]+:/ && !/^[ +-] / { in_deps = 0 }
      in_deps && /^\+ / { found = 1 }
      END { exit !found }'; then
    flag "Changes dependencies in pubspec.yaml."
  fi
fi

if (( ${#problems[@]} == 0 )); then
  echo "Nothing suspicious found."
  exit 0
fi

echo "Suspicious changes:"
printf '  - %s\n' "${problems[@]}"
if [[ "${REVIEWED:-false}" == "true" ]]; then
  echo "A maintainer marked this pull request security-reviewed; allowing it."
  exit 0
fi
echo
echo "A maintainer must review these and add the 'security-reviewed' label."
exit 1
