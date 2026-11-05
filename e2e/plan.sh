#!/usr/bin/env bash
# Plans the E2E matrix: one job per leg, with the Go and the slog-common version the leg tests. Prints the matrix as a
# JSON array, the include list of the test job in e2e.yml.
#   floor   the Go of go.mod's go directive (setup-go installs its newest patch release) and the slog-common version
#           go.mod requires
#   latest  the newest stable Go release on go.dev and the newest slog-common release on the Go module proxy
#   canary  the newest Go release candidate or beta when one is newer than that stable release, otherwise the stable
#           release again, and slog-common's newest pre-release when one is above its newest release
# Pushes and pull requests test the checkout. On the schedule (CLIENT_FROM=proxy) the client is the newest
# slog-betterstack release on the module proxy, and the floor comes from that release's go.mod instead.
# Env: LEG (all, floor, latest or canary), CLIENT_FROM (checkout or proxy), and GO_VERSION (e.g. 1.24.5 or 1.28rc1)
#      and SLOG_COMMON_VERSION (e.g. v0.21.0): exact versions that replace the planned ones in every leg.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROXY=https://proxy.golang.org
CLIENT_MODULE=github.com/BetterStackHQ/slog-betterstack
SLOG_COMMON=github.com/samber/slog-common
RELEASE='^v[0-9]+\.[0-9]+\.[0-9]+$'

# A lookup that fails at schedule time because of a hiccup should not file a Linear issue, so curl retries.
get() { curl -fsS -m 60 --retry 3 --retry-delay 10 "$1"; }

# The module proxy spells capital letters in a module path as "!" and the lower-case letter.
proxy_url() { echo "$PROXY/$(jq -Rr 'gsub("(?<c>[A-Z])"; "!" + (.c | ascii_downcase))' <<<"$1")/$2"; }

# The go directive and the version of the required module $2 in the go.mod on stdin.
go_directive() { sed -n 's/^go \([0-9][0-9.]*\)$/\1/p'; }
required() { awk -v module="$1" '($1 == module && $2 ~ /^v/) || ($1 == "require" && $2 == module) { print ($1 == "require" ? $3 : $2) }'; }

# Whether version $1 sorts above version $2.
newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n 1)" = "$1" ]; }

# The newest pre-release of module $1 above its newest release $2, or that release when there is none. v0.23.0-rc.1
# is a pre-release of v0.23.0: it counts when v0.23.0 is above the newest release.
newest_prerelease() {
  local versions candidate pick=""
  versions=$(get "$(proxy_url "$1" @v/list)") || return 1
  while read -r candidate; do
    [ -n "$candidate" ] || continue
    if newer "${candidate%%-*}" "$2" && { [ -z "$pick" ] || newer "$candidate" "$pick"; }; then pick=$candidate; fi
  done < <(grep -vE "$RELEASE" <<<"$versions" || true)
  echo "${pick:-$2}"
}

# setup-go takes a pre-release in semver form: 1.28rc1 is 1.28.0-rc.1.
setup_go_version() { sed -E 's/^([0-9]+\.[0-9]+)(beta|rc)([0-9]+)$/\1.0-\2.\3/' <<<"$1"; }

# The go.mod the floor comes from: the checkout's, or on the schedule the one of the release under test.
if [ "${CLIENT_FROM:-checkout}" = proxy ]; then
  CLIENT=$(get "$(proxy_url "$CLIENT_MODULE" @latest)" | jq -r .Version)
  GO_MOD=$(get "$(proxy_url "$CLIENT_MODULE" "@v/$CLIENT.mod")")
else
  CLIENT=checkout
  GO_MOD=$(cat "$ROOT/go.mod")
fi

LEGS=$([ "${LEG:-all}" = all ] && echo "floor latest canary" || echo "$LEG")
STABLE_GO=""
NEWEST_SLOG_COMMON=""
MATRIX='[]'
for leg in $LEGS; do
  if [ "$leg" != floor ] && [ -z "$STABLE_GO" ]; then
    STABLE_GO=$(get 'https://go.dev/dl/?mode=json' | jq -r '[.[] | select(.stable) | .version | ltrimstr("go")] | max_by(split(".") | map(tonumber))')
    NEWEST_SLOG_COMMON=$(get "$(proxy_url "$SLOG_COMMON" @latest)" | jq -r .Version)
  fi
  note=""
  case "$leg" in
    floor)
      go=$(go_directive <<<"$GO_MOD")
      slog_common=$(required "$SLOG_COMMON" <<<"$GO_MOD") ;;
    latest)
      go=$STABLE_GO
      slog_common=$NEWEST_SLOG_COMMON ;;
    canary)
      # Go publishes betas and release candidates only for the next minor, as go1.28rc1 or go1.28beta1.
      go=$(get 'https://go.dev/dl/?mode=json&include=all' | jq -r --arg stable "$STABLE_GO" '
        ($stable | split(".") | map(tonumber)) as $s
        | [.[] | select(.stable | not) | .version | ltrimstr("go")
            | capture("^(?<major>[0-9]+)\\.(?<minor>[0-9]+)(?<kind>beta|rc)(?<n>[0-9]+)$") as $v
            | {version: ., key: [($v.major | tonumber), ($v.minor | tonumber), (if $v.kind == "rc" then 1 else 0 end), ($v.n | tonumber)]}
            | select(.key[0:2] > $s[0:2])]
        | max_by(.key).version // empty')
      if [ -z "$go" ]; then go=$STABLE_GO; note=" as no pre-release is newer"; fi
      slog_common=$(newest_prerelease "$SLOG_COMMON" "$NEWEST_SLOG_COMMON") ;;
    *) echo "unknown leg $leg" >&2; exit 1 ;;
  esac
  if [ -n "${GO_VERSION:-}" ]; then go=$GO_VERSION; note=""; fi
  slog_common=${SLOG_COMMON_VERSION:-$slog_common}
  for value in "$go" "$slog_common" "$CLIENT"; do
    [ -n "$value" ] && [ "$value" != null ] || { echo "the $leg leg could not be planned: go '$go', slog-common '$slog_common', client '$CLIENT'" >&2; exit 1; }
  done
  MATRIX=$(jq -c --arg leg "$leg" --arg go "$go" --arg setup_go "$(setup_go_version "$go")" --arg note "$note" \
    --arg slog_common "$slog_common" --arg source "${CLIENT_FROM:-checkout}" --arg client "$CLIENT" '. + [{
      name: "\($leg) (Go \($go)\($note), slog-common \($slog_common))",
      leg: $leg,
      # go is what `go version` must name (1.21 also matches 1.21.13), setup_go what actions/setup-go installs.
      go: $go, setup_go: $setup_go,
      slog_common: $slog_common, source: $source, client: $client,
      # The floor runs on the oldest Ubuntu image GitHub offers, the other legs follow ubuntu-latest.
      runner: (if $leg == "floor" then "ubuntu-24.04" else "ubuntu-latest" end)
    }]' <<<"$MATRIX")
done
# Job names become artifact names (e2e-summary-<job name>), which must not contain any of these.
if jq -e 'any(.[].name; test("[\":<>|*?\\\\/]"))' <<<"$MATRIX" >/dev/null; then
  echo "a job name contains a character artifact names do not allow: $(jq -r '.[].name' <<<"$MATRIX")" >&2
  exit 1
fi
echo "$MATRIX"
