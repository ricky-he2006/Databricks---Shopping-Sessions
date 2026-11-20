#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
starter="$root/starter-duckdb"
work=$(realpath -m -- "${WORK_DIR:-$root/work}")
export CARGO_TARGET_DIR="$work/cargo-target"

usage() {
  cat <<'EOF'
Usage: bash builders-test.sh COMMAND [ARGS]
  setup                       Check local inputs and dependencies
  build                       Build the linux/amd64 scorer image
  fixtures                    Build and compare all official Docker fixtures
  generate EVENTS CLICKS SEED Generate a seeded dataset under work/datasets
  test DATASET                Run the built image and check for both reports
  bench DATASET [RUNS]         Repeat the Docker run and report wall times
  check-dev                   Run and compare the official 1M-event dev set
EOF
}

fail() {
  printf '%s\n' "$*" >&2
  exit 1
}

require() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command is unavailable: $1"
}

require_checker() {
  require python3
  python3 -c 'import polars' || fail 'Install Polars in the Python environment used by python3.'
}

build() {
  require docker
  require make
  make -C "$starter" build
}

generate() {
  local events=$1 clicks=$2 seed=$3
  [[ $events =~ ^[1-9][0-9]{3,}$ && $clicks =~ ^(0|[1-9][0-9]*)$ && $seed =~ ^(0|[1-9][0-9]*)$ ]] ||
    fail 'Expected at least 1,000 events and nonnegative integer click count and seed.'
  require cargo
  dataset="$work/datasets/seed_${seed}_events_${events}_clicks_${clicks}"
  [[ ! -e $dataset ]] || fail "Dataset already exists: $dataset"
  mkdir -p -- "$work/datasets"
  local pending
  pending=$(mktemp -d "$work/datasets/.generating.XXXXXXXX")
  if ! cargo run --locked --release --manifest-path "$root/generator/Cargo.toml" -- \
    --seed "$seed" --events "$events" --clicks "$clicks" --output "$pending"; then
    fail "Generation failed. Partial files are under $pending"
  fi
  # -T prevents a concurrent generation from nesting this directory under an existing dataset.
  mv -T -- "$pending" "$dataset"
  printf 'Dataset: %s\n' "$dataset"
}

run_container() {
  local input=$1
  [[ -f "$input/manifest.json" ]] || fail "Dataset manifest missing: $input/manifest.json"
  require docker
  mkdir -p -- "$work/runs" "$work/scratch"
  output=$(mktemp -d "$work/runs/run.XXXXXXXX")
  TMPDIR="$work/scratch" bash "$starter/scripts/docker_run.sh" "$input" "$output"

  local report files
  shopt -s nullglob
  for report in user_daily campaign_daily; do
    files=("$output/$report/"*.parquet)
    local found=0 file
    for file in "${files[@]}"; do
      if [[ -f $file && ! -L $file && ${file##*/} != _* ]]; then
        found=1
        break
      fi
    done
    (( found )) || fail "Missing $report Parquet output under $output"
  done
  printf 'Reports: %s\n' "$output"
}

case ${1:-help} in
  setup)
    [[ $# == 1 ]] || fail 'Usage: builders-test.sh setup'
    [[ -f "$starter/Dockerfile" && -f "$root/generator/Cargo.toml" &&
       -f "$root/dev-answer/user_daily.csv" && -f "$root/dev-answer/campaign_daily.csv" ]] ||
      fail 'Expected the unpacked DuckDB starter, generator, and dev answers beside this script.'
    require cargo
    require make
    require_checker
    require docker
    docker buildx version
    docker info --format '{{.ServerVersion}}'
    printf 'Local files and dependencies are available.\n'
    ;;
  build)
    [[ $# == 1 ]] || fail 'Usage: builders-test.sh build'
    build
    ;;
  fixtures)
    [[ $# == 1 ]] || fail 'Usage: builders-test.sh fixtures'
    require_checker
    build
    mkdir -p -- "$work/scratch"
    TMPDIR="$work/scratch" python3 "$starter/scripts/run_fixtures.py" "$starter/fixtures" -- bash "$starter/scripts/docker_run.sh"
    ;;
  generate)
    [[ $# == 4 ]] || fail 'Usage: builders-test.sh generate EVENTS CLICKS SEED'
    generate "$2" "$3" "$4"
    ;;
  test)
    [[ $# == 2 ]] || fail 'Usage: builders-test.sh test DATASET'
    run_container "$2"
    ;;
  bench)
    runs=${3:-3}
    [[ $# -ge 2 && $# -le 3 && $runs =~ ^[1-9][0-9]*$ ]] ||
      fail 'Usage: builders-test.sh bench DATASET [POSITIVE_RUNS]'
    TIMEFORMAT='Wall time: %3R seconds'
    for ((run = 1; run <= runs; run++)); do
      printf 'Run %s/%s\n' "$run" "$runs"
      time run_container "$2"
    done
    ;;
  check-dev)
    [[ $# == 1 ]] || fail 'Usage: builders-test.sh check-dev'
    require_checker
    build
    dataset="$work/datasets/seed_1_events_1000000_clicks_250000"
    if [[ ! -f "$dataset/manifest.json" ]]; then
      generate 1000000 250000 1
    fi
    run_container "$dataset"
    python3 "$starter/scripts/compare.py" "$output" "$root/dev-answer"
    ;;
  help|-h|--help) usage ;;
  *) usage >&2; exit 2 ;;
esac
