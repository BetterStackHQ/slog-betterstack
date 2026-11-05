#!/usr/bin/env bash
# Builds the example project with the leg's slog-common and proves that the build is the planned one.
#   e2e/example.sh build    resolves the modules and builds example-project into out/example
#   e2e/example.sh verify   checks the Go and the module versions compiled into out/example against the plan, prints
#                           them and appends them to out/versions.md
# From the checkout, the example builds as the package of this module that its doc comment tells customers to run, so
# it compiles the checkout's code. On the schedule it builds the way a customer's program does: as a module of its own,
# outside the checkout, that requires the slog-betterstack release under test from the Go module proxy.
# Env: LEG, CLIENT_FROM (checkout or proxy), CLIENT (the release under test on the schedule), PLANNED_GO (1.21, which
#      also matches 1.21.13, or 1.27.3 or 1.28rc1), PLANNED_SLOG_COMMON, RUNNER_TEMP (where the schedule's module goes).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=$ROOT/out
CLIENT_MODULE=github.com/BetterStackHQ/slog-betterstack
SLOG_COMMON=github.com/samber/slog-common
mkdir -p "$OUT"
if [ "$CLIENT_FROM" = checkout ]; then MODULE_DIR=$ROOT; else MODULE_DIR=$RUNNER_TEMP/example; fi

case "$1" in
  build)
    # Every go command's output goes to out/build.log too, and a failure ends with go's own last line.
    : > "$OUT/build.log"
    step() {
      local log status=0
      log=$("$@" 2>&1) || status=$?
      [ -z "$log" ] || echo "$log" | tee -a "$OUT/build.log"
      [ "$status" = 0 ] || { echo "::error::$* failed on $(go env GOVERSION): $(grep -v '^go: downloading' <<<"$log" | tail -n 1)"; exit 1; }
    }
    if [ "$CLIENT_FROM" = checkout ]; then
      cd "$ROOT"
      # A no-op on the floor, where go.mod already requires this version.
      step go get "$SLOG_COMMON@$PLANNED_SLOG_COMMON"
      step go build -o "$OUT/example" ./example-project
    else
      rm -rf "$MODULE_DIR"
      mkdir -p "$MODULE_DIR"
      cp "$ROOT"/example-project/*.go "$MODULE_DIR/"
      cd "$MODULE_DIR"
      step go mod init example
      # One go get, so that go fails instead of quietly moving one module when the two versions do not fit together.
      step go get "$CLIENT_MODULE@$CLIENT" "$SLOG_COMMON@$PLANNED_SLOG_COMMON"
      step go build -o "$OUT/example" .
    fi
    ;;

  verify)
    cd "$MODULE_DIR"
    # go version -m reads what the binary was built from: the Go release, the main module and every module compiled
    # in, one tab-separated line each ("mod" or "dep", path, version), with a "=>" line after a replaced one.
    BUILD=$(go version -m "$OUT/example")
    GO=$(sed -n '1s/^.*: //p' <<<"$BUILD")
    compiled() { awk -F '\t' -v kind="$1" -v module="$2" '$2 == kind && $3 == module { print $4 }' <<<"$BUILD"; }
    CLIENT_INFO=$(go list -m -json "$CLIENT_MODULE")
    SLOG_COMMON_VERSION=$(compiled dep "$SLOG_COMMON")
    if [ "$CLIENT_FROM" = checkout ]; then
      FROM="from the checkout at $(git -C "$ROOT" rev-parse --short HEAD)"
    else
      FROM="$(compiled dep "$CLIENT_MODULE") from the module proxy"
    fi
    INSTALLED="$GO $(go env GOOS)/$(go env GOARCH), slog-betterstack $FROM, slog-common $SLOG_COMMON_VERSION, samber/lo $(compiled dep github.com/samber/lo)"
    echo "- $INSTALLED" | tee -a "$OUT/versions.md" ${GITHUB_STEP_SUMMARY:+"$GITHUB_STEP_SUMMARY"}
    # The errors of the later steps name the versions: the Linear issue shows them when no read-back summary exists.
    [ -z "${GITHUB_ENV:-}" ] || echo "INSTALLED=$INSTALLED" >> "$GITHUB_ENV"
    FAILED=0
    error() { echo "::error::$1"; FAILED=1; }

    case "$GO" in
      "go$PLANNED_GO" | "go$PLANNED_GO".*) ;;
      *) error "The example was built with $GO, but the $LEG leg plans Go $PLANNED_GO." ;;
    esac
    if [ "$CLIENT_FROM" = checkout ]; then
      # The client is the main module, in the checkout's directory, not a release from the module proxy.
      if [ "$(compiled mod "$CLIENT_MODULE")" = "" ] || [ "$(jq -r '.Main == true and .Dir == $root' --arg root "$ROOT" <<<"$CLIENT_INFO")" != true ]; then
        error "The example was not built from the checkout: go list -m reports $(jq -c '{Path, Version, Main, Dir}' <<<"$CLIENT_INFO")."
      fi
    else
      # The client is exactly the planned release, in the module cache and not replaced.
      if [ "$(compiled dep "$CLIENT_MODULE")" != "$CLIENT" ] || grep -q $'^\t=>' <<<"$BUILD" \
        || [ "$(jq -r '.Version == $client and .Replace == null and (.Dir | startswith($cache))' --arg client "$CLIENT" --arg cache "$(go env GOMODCACHE)/" <<<"$CLIENT_INFO")" != true ]; then
        error "The example was not built with slog-betterstack $CLIENT from the module proxy: go list -m reports $(jq -c '{Path, Version, Dir, Replace}' <<<"$CLIENT_INFO")."
      fi
    fi
    # Every leg asks go get for one exact version, so every leg checks it, the canary too.
    if [ "$SLOG_COMMON_VERSION" != "$PLANNED_SLOG_COMMON" ]; then
      error "The example was built with slog-common $SLOG_COMMON_VERSION, but the $LEG leg plans $PLANNED_SLOG_COMMON."
    fi
    exit "$FAILED"
    ;;

  *) echo "usage: e2e/example.sh build|verify" >&2; exit 1 ;;
esac
