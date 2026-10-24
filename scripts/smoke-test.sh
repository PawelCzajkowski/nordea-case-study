#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: bash scripts/smoke-test.sh [--keep] [--help]

Build and check an isolated local Compose environment, then remove its containers,
network, and volumes. --keep retains the environment on success or failure.

Requirements: Bash 3.2+, Docker Compose 2.24.4+, curl, jq, uuidgen.
Environment:
  SMOKE_PORT             Host port for the API (default: 0, automatically assigned).
  SMOKE_STARTUP_TIMEOUT  Service startup/health timeout in seconds (default: 180).
  SMOKE_PROCESS_TIMEOUT  Event processing timeout in seconds (default: 60).
  SMOKE_HTTP_TIMEOUT     Timeout for each HTTP request in seconds (default: 15).

See docs/smoke-test.md for the equivalent manual procedure.
USAGE
}

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

smoke_keep=0
for smoke_argument in "$@"; do
    case "$smoke_argument" in
        --keep) smoke_keep=1 ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; fail "Unknown argument: $smoke_argument" ;;
    esac
done

for smoke_dependency in docker curl jq uuidgen; do
    command -v "$smoke_dependency" >/dev/null 2>&1 || fail "Missing command: $smoke_dependency"
done

SMOKE_STARTUP_TIMEOUT="${SMOKE_STARTUP_TIMEOUT:-180}"
SMOKE_PROCESS_TIMEOUT="${SMOKE_PROCESS_TIMEOUT:-60}"
SMOKE_HTTP_TIMEOUT="${SMOKE_HTTP_TIMEOUT:-15}"
SMOKE_PORT="${SMOKE_PORT:-0}"
for smoke_timeout in "$SMOKE_STARTUP_TIMEOUT" "$SMOKE_PROCESS_TIMEOUT" "$SMOKE_HTTP_TIMEOUT"; do
    [[ "$smoke_timeout" =~ ^[1-9][0-9]{0,4}$ ]] || fail "Timeouts must be positive integers of at most five digits"
done
[[ "$SMOKE_PORT" =~ ^(0|[1-9][0-9]{0,4})$ ]] || fail "SMOKE_PORT must be an integer between 0 and 65535"
(( SMOKE_PORT <= 65535 )) || fail "SMOKE_PORT must be an integer between 0 and 65535"
export SMOKE_PORT

docker compose version >/dev/null 2>&1 || fail "Docker Compose is required"
docker info >/dev/null 2>&1 || fail "Docker is unavailable; start Docker Desktop or the Docker daemon"

smoke_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export SMOKE_PROJECT_NAME="nordea-smoke-$(date -u +%Y%m%d%H%M%S)-$$"
smoke_compose=(docker compose --project-directory "$smoke_root" --project-name "$SMOKE_PROJECT_NAME"
    --file "$smoke_root/docker-compose.yml" --file "$smoke_root/docker-compose.smoke.yml")
"${smoke_compose[@]}" config --quiet || fail "Invalid Compose configuration; Docker Compose 2.24.4+ is required"

# Refuse to touch a project that already has containers, even in the unlikely event of a name collision.
smoke_existing="$(docker ps --all --quiet --filter "label=com.docker.compose.project=$SMOKE_PROJECT_NAME")"
[[ -z "$smoke_existing" ]] || fail "Compose project already exists: $SMOKE_PROJECT_NAME"

print_cleanup_command() {
    printf 'SMOKE_PROJECT_NAME=%q ' "$SMOKE_PROJECT_NAME"
    printf '%q ' "${smoke_compose[@]}" down --volumes --remove-orphans
    printf '\n'
}

cleanup() {
    local smoke_exit=$?
    trap - EXIT INT TERM
    set +e
    if (( smoke_exit != 0 )); then
        printf '\nFailed step diagnostics for %s:\n' "$SMOKE_PROJECT_NAME" >&2
        "${smoke_compose[@]}" ps --all >&2
        "${smoke_compose[@]}" logs --no-color --tail=80 app kafka postgres >&2
    fi
    if (( smoke_keep == 1 )); then
        printf '\nEnvironment retained. To remove it, run:\n'
        print_cleanup_command
    else
        printf '\nRemoving smoke test containers, network, and volumes...\n'
        if ! "${smoke_compose[@]}" down --volumes --remove-orphans; then
            printf 'ERROR: Cleanup failed. Retry:\n' >&2
            print_cleanup_command >&2
            smoke_exit=1
        fi
    fi
    if (( smoke_exit == 0 )); then
        printf '\nSMOKE TEST PASSED\n'
    else
        printf '\nSMOKE TEST FAILED (exit %s)\n' "$smoke_exit" >&2
    fi
    exit "$smoke_exit"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'Compose project: %s\n' "$SMOKE_PROJECT_NAME"
printf '[1/7] Building and starting PostgreSQL, Kafka, and the application...\n'
"${smoke_compose[@]}" up --detach --build --wait --wait-timeout "$SMOKE_STARTUP_TIMEOUT"
smoke_address="$("${smoke_compose[@]}" port app 8080)"
[[ "$smoke_address" == 127.0.0.1:* ]] || fail "Expected a loopback API address, got: $smoke_address"
smoke_base_url="http://$smoke_address"
printf 'API URL: %s\n' "$smoke_base_url"

smoke_status=000
smoke_body=''
http_request() {
    local smoke_method=$1 smoke_path=$2 smoke_token=$3
    local smoke_response
    local smoke_curl=(curl --silent --show-error --connect-timeout 3 --max-time "$SMOKE_HTTP_TIMEOUT"
        --request "$smoke_method" --write-out $'\n%{http_code}')
    if [[ -n "$smoke_token" ]]; then
        smoke_curl+=(--header "Authorization: Bearer $smoke_token")
    fi
    if (( $# == 4 )); then
        smoke_curl+=(--header 'Content-Type: application/json' --data-binary "$4")
    fi
    if ! smoke_response="$("${smoke_curl[@]}" "$smoke_base_url$smoke_path")"; then
        smoke_status=000
        smoke_body='HTTP request could not be completed'
        return 1
    fi
    smoke_status="${smoke_response##*$'\n'}"
    smoke_body="${smoke_response%$'\n'*}"
}

expect_status() {
    local smoke_expected=$1
    shift
    http_request "$@" || fail "$1 $2 failed: $smoke_body"
    [[ "$smoke_status" == "$smoke_expected" ]] || fail "$1 $2: expected HTTP $smoke_expected, got $smoke_status: $smoke_body"
}

printf '[2/7] Waiting for public health, liveness, and readiness...\n'
for smoke_path in /actuator/health /actuator/health/liveness /actuator/health/readiness; do
    smoke_deadline=$((SECONDS + SMOKE_STARTUP_TIMEOUT))
    while true; do
        if http_request GET "$smoke_path" '' 2>/dev/null && [[ "$smoke_status" == 200 ]] &&
                jq -e '.status == "UP"' >/dev/null 2>&1 <<<"$smoke_body"; then
            printf '  %s: HTTP 200, UP\n' "$smoke_path"
            break
        fi
        (( SECONDS < smoke_deadline )) || fail "$smoke_path did not become healthy: HTTP $smoke_status: $smoke_body"
        sleep 2
    done
done

printf '[3/7] Checking that missing and invalid authentication return HTTP 401...\n'
expect_status 401 GET /api/v1/messages ''
expect_status 401 GET /api/v1/messages invalid-smoke-token

smoke_message_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
[[ "$smoke_message_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || fail "uuidgen returned an invalid UUID"
smoke_timestamp="$(date -u +%Y-%m-%dT%H:%M:%S)"
smoke_event="$(jq -n --arg id "$smoke_message_id" --arg occurredAt "$smoke_timestamp.123456789Z" \
    '{messageId: $id, type: "smoke.created", schemaVersion: "1", occurredAt: $occurredAt,
      payload: {customerId: ("smoke-" + $id)}}')"
printf '[4/7] Publishing Event %s and checking HTTP 202...\n' "$smoke_message_id"
expect_status 401 POST /api/v1/events '' "$smoke_event"
expect_status 202 POST /api/v1/events local-dev-token "$smoke_event"
jq -e --arg id "$smoke_message_id" '.messageId == $id' >/dev/null <<<"$smoke_body" || fail "Publish response has an unexpected messageId"

assert_stored_event() {
    jq -e --arg id "$smoke_message_id" --arg time "$smoke_timestamp.123457Z" '
        .totalElements == 1 and .page == 0 and .size == 100 and
        (.content | length == 1) and
        (.content[0] | .messageId == $id and .type == "smoke.created" and .schemaVersion == "1" and
         .occurredAt == $time and .payload == {customerId: ("smoke-" + $id)} and
         (.receivedAt | type == "string"))' >/dev/null <<<"$smoke_body" || fail "Unexpected stored Event: $smoke_body"
}

printf '[5/7] Waiting for HTTP -> Kafka -> PostgreSQL -> HTTP processing...\n'
smoke_deadline=$((SECONDS + SMOKE_PROCESS_TIMEOUT))
while true; do
    if http_request GET '/api/v1/messages?page=0&size=100' local-dev-token && [[ "$smoke_status" == 200 ]] &&
            jq -e --arg id "$smoke_message_id" 'any(.content[]; .messageId == $id)' >/dev/null 2>&1 <<<"$smoke_body"; then
        break
    fi
    (( SECONDS < smoke_deadline )) || fail "Event was not processed within $SMOKE_PROCESS_TIMEOUT seconds: HTTP $smoke_status: $smoke_body"
    sleep 1
done
assert_stored_event

printf '[6/7] Repeating the Event and checking idempotency and durable storage...\n'
expect_status 202 POST /api/v1/events local-dev-token "$smoke_event"
jq -e --arg id "$smoke_message_id" '.messageId == $id' >/dev/null <<<"$smoke_body" || fail "Retry response has an unexpected messageId"
expect_status 200 GET '/api/v1/messages?page=0&size=100' local-dev-token
assert_stored_event
smoke_count="$("${smoke_compose[@]}" exec -T postgres psql -X -U message_service -d messages -Atc \
    "SELECT count(*) FROM processed_messages WHERE message_id = '$smoke_message_id';")"
[[ "$smoke_count" == 1 ]] || fail "Expected one persisted Event, got: $smoke_count"

printf '[7/7] Checking that changed content returns HTTP 409 and preserves the original...\n'
smoke_conflict="$(jq '.payload.customerId = "smoke-conflict"' <<<"$smoke_event")"
expect_status 409 POST /api/v1/events local-dev-token "$smoke_conflict"
expect_status 200 GET '/api/v1/messages?page=0&size=100' local-dev-token
assert_stored_event
