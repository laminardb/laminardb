#!/usr/bin/env bash
# FIX desk demo driver; see README.md.
#   ./run.sh venue [venue options]   start the local broker simulator
#   ./run.sh server [venue]          start LaminarDB on venues/<venue>.sql (default: simulator)
#   ./run.sh watch                   live terminal view of the desk
#   ./run.sh reconcile               venue ledger against the Delta Lake audit trail
#   ./run.sh reset                   delete all demo state (venue, journal, checkpoints, audit)
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)
DATA=${DESK_DATA_DIR:-$HERE/data}
# Probe by running: on Windows `python3` may be a Store alias that only opens the Store.
PYTHON=${PYTHON:-$(for p in python3 python; do "$p" -c '' 2>/dev/null && echo "$p" && break; done)}
# The standard FIX 4.4 dictionary from the QuickFIX commit the engine is built from.
QUICKFIX_COMMIT=386ce46e917ae494ab6e90b1be90fd421cdbe3f9
FIX44_SHA256=a82655b54363aa9c6d1b2f21f294f1198c0d7125d7b44c26d93d3179f3358425

# Native tools on Windows need C:/ paths, not Git Bash /c/ paths.
native() {
    if command -v cygpath >/dev/null; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

sha256() {
    if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

fetch_dictionary() {
    local dictionary="$HERE/dictionaries/FIX44.xml"
    [ -f "$dictionary" ] && return
    mkdir -p "$HERE/dictionaries"
    curl -sSfL -o "$dictionary.tmp" \
        "https://raw.githubusercontent.com/quickfix/quickfix/$QUICKFIX_COMMIT/spec/FIX44.xml"
    if [ "$(sha256 "$dictionary.tmp")" != "$FIX44_SHA256" ]; then
        rm -f "$dictionary.tmp"
        echo "FIX44.xml checksum mismatch" >&2
        exit 1
    fi
    mv "$dictionary.tmp" "$dictionary"
}

venue() {
    fetch_dictionary
    cd "$REPO"
    exec cargo run --release -p laminar-connectors --no-default-features --features fix,testing \
        --example fix_venue -- \
        --dictionary "$(native "$HERE/dictionaries/FIX44.xml")" --data "$(native "$DATA/venue")" "$@"
}

server() {
    local venue=${1:-simulator}
    [ -f "$HERE/venues/$venue.sql" ] || { echo "no venues/$venue.sql" >&2; exit 1; }
    local binary="$REPO/target/release/laminardb"
    [ -x "$binary" ] || [ -x "$binary.exe" ] || {
        echo "build the server first: cargo build --release -p laminar-server --features fix" >&2
        exit 1
    }
    fetch_dictionary
    mkdir -p "$DATA"
    DESK_DATA=$(native "$DATA")
    DESK_DICTIONARIES=$(native "$HERE/dictionaries")
    case "$DESK_DATA" in
        /*) DESK_CHECKPOINT_URL="file://$DESK_DATA/checkpoints" ;;
        *) DESK_CHECKPOINT_URL="file:///$DESK_DATA/checkpoints" ;;
    esac
    export DESK_DATA DESK_DICTIONARIES DESK_CHECKPOINT_URL
    # The SQL keeps its ${VAR} references for the DDL to resolve; `$${` escapes them from the
    # server's own config substitution. Durable DDL may not contain comments.
    local config="$DATA/laminardb.toml"
    {
        echo "sql = '''"
        sed -e '/^[[:space:]]*--/d' -e 's/\${/$${/g' "$HERE/venues/$venue.sql" "$HERE/desk.sql"
        echo "'''"
        echo
        cat "$HERE/server.toml"
    } >"$config"
    exec "$binary" --config "$(native "$config")"
}

case "${1:-}" in
    venue) shift; venue "$@" ;;
    server) shift; server "$@" ;;
    watch) shift; exec "$PYTHON" "$HERE/watch.py" "$@" ;;
    reconcile) DESK_DATA=$(native "$DATA") exec "${DUCKDB:-duckdb}" -c ".read '$(native "$HERE/reconcile.sql")'" ;;
    reset) rm -rf "$DATA" ;;
    *) sed -n '2,7p' "$0"; exit 1 ;;
esac
