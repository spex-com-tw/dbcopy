#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not a git repository: $ROOT_DIR" >&2
  exit 1
fi

if ! git diff --quiet --ignore-submodules HEAD -- || ! git diff --cached --quiet --ignore-submodules --; then
  echo "Working tree has uncommitted changes. Commit or stash before releasing." >&2
  exit 1
fi

if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "Working tree has untracked files. Commit, ignore, or clean them before releasing." >&2
  exit 1
fi

latest_local_tag="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 1 || true)"
latest_remote_tag="$(git ls-remote --tags --refs origin 'v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null | awk -F'/' '{print $3}' | sort -V | tail -n 1 || true)"

if [[ -n "$latest_local_tag" && -n "$latest_remote_tag" ]]; then
  latest_tag="$(printf '%s\n%s\n' "$latest_local_tag" "$latest_remote_tag" | sort -V | tail -n 1)"
elif [[ -n "$latest_local_tag" ]]; then
  latest_tag="$latest_local_tag"
else
  latest_tag="$latest_remote_tag"
fi

if [[ -n "$latest_tag" && "$latest_tag" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  suggested_tag="v${major}.${minor}.$((patch + 1))"
else
  suggested_tag="v1.0.0"
fi

read -r -p "Release tag [${suggested_tag}]: " input_tag
tag="${input_tag:-$suggested_tag}"

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid tag format: $tag (expected: v<major>.<minor>.<patch>)" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  echo "Tag already exists locally: $tag" >&2
  exit 1
fi

if git ls-remote --tags --refs origin "refs/tags/$tag" | grep -q .; then
  echo "Tag already exists on origin: $tag" >&2
  exit 1
fi

echo "Creating tag: $tag"
git tag "$tag"

echo "Pushing tag to origin: $tag"
git push origin "$tag"

echo "Done. Triggered GitHub release workflow with tag: $tag"
