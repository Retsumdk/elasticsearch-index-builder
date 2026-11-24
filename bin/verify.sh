#!/usr/bin/env bash
# One command that proves the repository is healthy: lint, format, types, tests,
# the demo transcript, both examples, and a wheel that installs and runs.
set -uo pipefail

cd "$(dirname "$0")/.."
failures=0

step() {
  printf '\n== %s\n' "$1"
}

check() {
  local label="$1"
  shift
  if "$@"; then
    printf '   ok   %s\n' "$label"
  else
    printf '   FAIL %s\n' "$label"
    failures=$((failures + 1))
  fi
}

step "1/7 lint"
check "ruff check" python3 -m ruff check src tests examples

step "2/7 format"
check "ruff format --check" python3 -m ruff format --check src tests examples

step "3/7 types"
check "mypy --strict" python3 -m mypy

step "4/7 tests"
check "pytest (coverage floor 90)" python3 -m pytest -q --cov=esib --cov-fail-under=90

step "5/7 demo transcript"
git_demo="$(mktemp)"
python3 -m esib demo >"$git_demo" 2>&1
if diff -q demo-output.txt "$git_demo" >/dev/null; then
  printf '   ok   demo-output.txt matches the code\n'
else
  printf '   FAIL demo-output.txt drifted from the code\n'
  diff -u demo-output.txt "$git_demo" || true
  failures=$((failures + 1))
fi
rm -f "$git_demo"

step "6/7 examples"
check "index_csv_catalog.py" python3 examples/index_csv_catalog.py
check "zero_downtime_rebuild.py" python3 examples/zero_downtime_rebuild.py

step "7/7 wheel"
build_dir="$(mktemp -d)"
venv_dir="$(mktemp -d)"
if python3 -m build --outdir "$build_dir" >/dev/null 2>&1; then
  printf '   ok   sdist and wheel built\n'
  if python3 -m venv "$venv_dir" >/dev/null 2>&1 &&
    "$venv_dir/bin/pip" install -q "$build_dir"/*.whl >/dev/null 2>&1; then
    printf '   ok   wheel installs into a clean venv\n'
    if (cd /tmp && "$venv_dir/bin/esib" demo >"$build_dir/from-wheel.txt" 2>&1) &&
      diff -q demo-output.txt "$build_dir/from-wheel.txt" >/dev/null; then
      printf '   ok   installed package reproduces the demo\n'
    else
      printf '   FAIL installed package diverged from the demo\n'
      failures=$((failures + 1))
    fi
  else
    printf '   FAIL wheel would not install\n'
    failures=$((failures + 1))
  fi
else
  printf '   FAIL python -m build\n'
  failures=$((failures + 1))
fi
rm -rf "$build_dir" "$venv_dir"

printf '\n'
if [ "$failures" -eq 0 ]; then
  printf 'RESULT: PASS\n'
  exit 0
fi
printf 'RESULT: FAIL (%d step(s))\n' "$failures"
exit 1
