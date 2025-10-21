#!/usr/bin/env bash
# Install repository hooks into .git/hooks
# Usage: ./scripts/install-hooks.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOKS_SRC="$REPO_ROOT/.githooks"
HOOKS_DST="$REPO_ROOT/.git/hooks"

if [ ! -d "$HOOKS_SRC" ]; then
  echo "No .githooks directory found in repository root." >&2
  exit 1
fi

if [ ! -d "$HOOKS_DST" ]; then
  echo "This does not appear to be a git repository (no .git/hooks)." >&2
  exit 1
fi

for hook in "$HOOKS_SRC"/*; do
  hook_name=$(basename "$hook")
  dst="$HOOKS_DST/$hook_name"
  echo "Installing $hook_name -> $dst"
  cp "$hook" "$dst"
  chmod +x "$dst"
done

echo "Hooks installed."
