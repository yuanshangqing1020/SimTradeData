#!/usr/bin/env bash
# Export data from DuckDB and release to GitHub.
# Usage: bash scripts/release_data.sh [--market cn|us|all]
#
# This script:
# 1. Runs export_parquet.py → data/export/{market}/
# 2. Packages into a single tar.gz
# 3. Creates/updates a GitHub Release on this repo
#
# Prerequisites: poetry install, gh auth login

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
MARKET="cn"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --market) MARKET="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

MARKET=$(echo "$MARKET" | tr '[:upper:]' '[:lower:]')
if [[ "$MARKET" != "cn" && "$MARKET" != "us" && "$MARKET" != "all" ]]; then
  echo "ERROR: --market must be cn, us, or all"
  exit 1
fi

release_market() {
  local market="$1"
  local export_dir="$PROJECT_ROOT/data/export/$market"

  # 1. Export
  echo "=== Exporting $market data ==="
  cd "$PROJECT_ROOT"
  poetry run python scripts/export_parquet.py --market "$market"

  # Read version from manifest
  local manifest="$export_dir/manifest.json"
  if [ ! -f "$manifest" ]; then
    echo "ERROR: Export did not produce manifest.json"
    return 1
  fi

  local version
  version=$(python3 -c "import json; print(json.load(open('$manifest'))['version'])")
  local tag="data-${market}-${version}"
  local archive="/tmp/simtradelab-data-${market}-${version}.tar.gz"

  # 2. Package
  echo ""
  echo "=== Packaging ${market} ${version} ==="
  tar -czf "$archive" -C "$export_dir" .

  local size
  size=$(ls -lh "$archive" | awk '{print $5}')
  echo "  -> $archive ($size)"

  # 3. Release
  echo ""
  echo "=== Uploading to GitHub ==="
  if gh release view "$tag" >/dev/null 2>&1; then
    echo "  Release $tag exists, updating..."
    gh release upload "$tag" "$archive" --clobber
  else
    gh release create "$tag" \
      --title "SimTradeData ${market} ${version}" \
      --notes "Data date: ${version} (${market})" \
      "$archive"
  fi

  rm -f "$archive"
  echo ""
  echo "=== Done: $(gh release view "$tag" --json url -q .url) ==="
  echo ""
}

if [ "$MARKET" = "all" ]; then
  release_market "cn"
  release_market "us"
else
  release_market "$MARKET"
fi
