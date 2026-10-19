-- What the sender did with the notifications of a load-test run.
-- Usage: make loadtest-report RUN=<run id printed by the load test>
-- Rows of one run have idempotency keys that start with "<run>-".
\echo == rows by channel and state
SELECT channel, state, count(*) AS rows
FROM notifications
WHERE idempotency_key LIKE :'run' || '-%'
GROUP BY channel, state
ORDER BY channel, state;

-- The clock starts when the sender inserts the row, so Kafka lag before the
-- sender reads a message is not in these numbers; the load test report's
-- latency covers only the dispatcher's 202.
\echo == time from the insert to the provider accepting it, in ms (accepted rows)
SELECT channel,
       count(*) AS rows,
       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY ms))::numeric, 1) AS p50,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY ms))::numeric, 1) AS p95,
       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY ms))::numeric, 1) AS p99,
       round(max(ms)::numeric, 1) AS max,
       count(*) FILTER (WHERE ms > 5000) AS over_5s
FROM (
    SELECT channel, extract(epoch FROM accepted_at - created_at) * 1000 AS ms
    FROM notifications
    WHERE idempotency_key LIKE :'run' || '-%' AND accepted_at IS NOT NULL
) t
GROUP BY channel
ORDER BY channel;

\echo == accepted rows by provider (fallback shows as the second provider)
SELECT channel, provider, count(*) AS rows
FROM notifications
WHERE idempotency_key LIKE :'run' || '-%' AND state = 'accepted'
GROUP BY channel, provider
ORDER BY channel, provider;

\echo == rows the janitor had to touch
SELECT count(*) FILTER (WHERE attempts > 0) AS redriven,
       count(*) FILTER (WHERE state = 'unresolved') AS unresolved
FROM notifications
WHERE idempotency_key LIKE :'run' || '-%';
