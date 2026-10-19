#!/usr/bin/env bash
# Runs the load test while a provider misbehaves and prints what the system did.
#
#   SMS_A_ERROR_RATE=1 infra/provider-failure.sh
#   SMS_A_LATENCY=3s   infra/provider-failure.sh        # above the 2s timeout
#   SMS_A_RPS=100      infra/provider-failure.sh        # rate limited
#   SMS_A_ERROR_RATE=1 SMS_B_ERROR_RATE=1 infra/provider-failure.sh   # both down
#
# The provider settings are the compose variables of the stack. The script
# empties the notifications table, recreates the sender and janitor with the
# settings (which also resets their circuit breakers), offers RATE requests per
# second for DURATION, waits until nothing is pending (up to SETTLE seconds, so
# the janitor gets a chance), then prints the sender's attempt counts and the
# report for the run. Needs `make stack-up` first. The attempt counts are read
# from the sender's JSON log by field order and by the words "circuit breaker
# open", so they go blank if that log call or error text changes.
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-600}
DURATION=${DURATION:-30s}
SETTLE=${SETTLE:-120}
compose="docker compose -f infra/docker-compose.yml"
psql="$compose exec -T postgres psql -U eshaar -d eshaar -Atc"

# Stop the consumers first so nothing re-inserts rows while the table is emptied.
$compose --profile stack stop sender janitor >/dev/null 2>&1
$psql "truncate notifications cascade" >/dev/null 2>&1
$compose --profile stack up -d --force-recreate sender janitor >/dev/null 2>&1
sleep 5
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)

out=$(make --no-print-directory loadtest LOADTEST_ARGS="-rates $RATE -step $DURATION -channels sms,email" | tail -3)
echo "$out"
run=$(echo "$out" | sed -n 's/^run \([0-9a-f]*\) .*/\1/p')
sent=$(echo "$out" | tail -1 | awk '{print $3}')
[ -n "$run" ] || { echo "could not read the run id from the load test output" >&2; exit 1; }

start=$(date +%s)
pending=$($psql "select count(*) from notifications where idempotency_key like '$run-%' and state = 'pending'")
while (( $(date +%s) - start < SETTLE )); do
  [ "$pending" = 0 ] && break
  sleep 2
  pending=$($psql "select count(*) from notifications where idempotency_key like '$run-%' and state = 'pending'")
done
echo "settled after $(( $(date +%s) - start ))s more, pending=$pending, requests sent=$sent"

echo "== provider attempts that did not succeed (sender log), by provider and outcome"
# grep finds nothing when the run was clean, which must not stop the script.
{ $compose logs --no-log-prefix --since "$since" sender 2>&1 || true; } |
  { grep '"msg":"provider attempt"' || true; } |
  sed -n -e 's/.*"provider":"\([^"]*\)","outcome":"\([^"]*\)".*circuit breaker open.*/\1 \2 (rejected by the open breaker, no call made)/p' \
         -e '/circuit breaker open/!s/.*"provider":"\([^"]*\)","outcome":"\([^"]*\)".*/\1 \2 (call made)/p' | sort | uniq -c
echo "== other warnings (sender and janitor)"
{ $compose logs --no-log-prefix --since "$since" sender janitor 2>&1 || true; } |
  { grep '"level":"WARN"' || true; } | { grep -v '"msg":"provider attempt"' || true; } |
  sed -n 's/.*"msg":"\([^"]*\)".*/\1/p' | sort | uniq -c
# Report sections 2 and 3 are the timing and provider tables.
make -s loadtest-report RUN="$run" 2>&1 | grep -v '^docker compose'
