#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$ROOT_DIR/DbCopy.csproj"
PUBLISH_DIR="$ROOT_DIR/publish"

RIDS=(
  "win-x64"
  "linux-x64"
  "linux-arm64"
  "osx-x64"
  "osx-arm64"
)

if [[ ! -f "$PROJECT" ]]; then
  echo "Project not found: $PROJECT" >&2
  exit 1
fi

echo "Cleaning publish directory: $PUBLISH_DIR"
rm -rf "$PUBLISH_DIR"
mkdir -p "$PUBLISH_DIR"

for rid in "${RIDS[@]}"; do
  echo "Publishing for $rid..."
  dotnet publish "$PROJECT" \
    -c Release \
    -r "$rid" \
    --self-contained true \
    -p:PublishSingleFile=true \
    -p:EnableCompressionInSingleFile=true \
    -o "$PUBLISH_DIR/$rid"
done

echo "Done. Artifacts are in: $PUBLISH_DIR"
