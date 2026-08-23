#!/usr/bin/env bash
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
demo_dir="$(cd "${script_dir}/.." && pwd)"
repo_root="$(cd "${demo_dir}/../../.." && pwd)"
smoke_port="${MARKOUT_LAB_SMOKE_PORT:-18089}"
player_id="markout-smoke-player"
temp_dir="$(mktemp -d)"
server_log="${temp_dir}/server.log"
events_log="${temp_dir}/events.log"
health_json="${temp_dir}/health.json"
demo_pid=""
events_pid=""

cleanup() {
  if [[ -n "${events_pid}" ]] && kill -0 "${events_pid}" 2>/dev/null; then
    kill "${events_pid}" 2>/dev/null || true
    wait "${events_pid}" 2>/dev/null || true
  fi
  if [[ -n "${demo_pid}" ]] && kill -0 "${demo_pid}" 2>/dev/null; then
    kill -TERM "${demo_pid}" 2>/dev/null || true
    wait "${demo_pid}" 2>/dev/null || true
  fi
  rm -rf "${temp_dir}"
}
trap cleanup EXIT

cd "${repo_root}"
cargo build --manifest-path "${demo_dir}/Cargo.toml" --locked

target_dir="${CARGO_TARGET_DIR:-${demo_dir}/target}"
binary="${target_dir}/debug/markout-lab"
if [[ -x "${binary}.exe" ]]; then
  binary="${binary}.exe"
fi

demo_args=(--port "${smoke_port}")
if [[ -n "${MARKOUT_LAB_FEED_URL:-}" ]]; then
  demo_args+=(--feed-url "${MARKOUT_LAB_FEED_URL}")
fi
"${binary}" "${demo_args[@]}" >"${server_log}" 2>&1 &
demo_pid=$!

healthy=false
for _ in $(seq 1 120); do
  if curl --fail --silent "http://127.0.0.1:${smoke_port}/api/health" >"${health_json}" \
    && grep -q '"ok":true' "${health_json}"; then
    healthy=true
    break
  fi
  if ! kill -0 "${demo_pid}" 2>/dev/null; then
    cat "${server_log}"
    echo "Markout Lab stopped because its required live feed was unavailable" >&2
    exit 1
  fi
  sleep 0.25
done
if [[ "${healthy}" != true ]]; then
  cat "${health_json}" 2>/dev/null || true
  cat "${server_log}"
  echo "Timed out waiting for a healthy live exchange feed" >&2
  exit 1
fi

curl --silent --no-buffer --max-time 50 \
  "http://127.0.0.1:${smoke_port}/events?player_id=${player_id}" >"${events_log}" &
events_pid=$!
sleep 0.5

order_response="$(curl --fail-with-body --silent \
  -H 'content-type: application/json' \
  -d "{\"player_id\":\"${player_id}\",\"side\":\"BUY\",\"quantity\":0.001}" \
  "http://127.0.0.1:${smoke_port}/api/orders")"
if [[ "${order_response}" != *'"accepted":true'* ]]; then
  echo "Unexpected simulated-order response: ${order_response}" >&2
  exit 1
fi

observed=false
for _ in $(seq 1 200); do
  horizons=true
  for horizon in 0 1000 5000 15000 30000; do
    if ! grep -q "\"horizon_ms\":${horizon}" "${events_log}"; then
      horizons=false
      break
    fi
  done
  if grep -q '^event: fill' "${events_log}" && [[ "${horizons}" == true ]]; then
    observed=true
    break
  fi
  if ! kill -0 "${demo_pid}" 2>/dev/null; then
    cat "${server_log}"
    echo "Markout Lab stopped before all live-market horizons arrived" >&2
    exit 1
  fi
  sleep 0.25
done
if [[ "${observed}" != true ]]; then
  cat "${events_log}"
  echo "Did not observe the simulated fill and all five live-market horizons" >&2
  exit 1
fi

kill "${events_pid}" 2>/dev/null || true
wait "${events_pid}" 2>/dev/null || true
events_pid=""
kill -TERM "${demo_pid}"
wait "${demo_pid}"
demo_pid=""

echo "Live Markout Lab smoke test passed"
