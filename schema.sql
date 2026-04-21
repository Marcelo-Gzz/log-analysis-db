-- ============================================================
--  Log Analysis Database — Schema, Indexes & Views
--  Compatible with: PostgreSQL 14+
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  1. RAW INGEST TABLE
--  Holds unparsed log lines before processing
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_log_ingest (
    id          BIGSERIAL PRIMARY KEY,
    raw_line    TEXT        NOT NULL,
    source      VARCHAR(64),                  -- e.g. 'nginx', 'app', 'system'
    ingested_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    processed   BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_raw_unprocessed ON raw_log_ingest (processed)
    WHERE processed = FALSE;


-- ────────────────────────────────────────────────────────────
--  2. MAIN LOG TABLE  (range-partitioned by day)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS web_logs (
    id          BIGSERIAL,
    ts          TIMESTAMP   NOT NULL,
    level       VARCHAR(8)  NOT NULL CHECK (level IN ('DEBUG','INFO','WARN','ERROR')),
    method      VARCHAR(8),
    path        VARCHAR(512),
    status_code SMALLINT,
    duration_ms INT,
    ip_address  VARCHAR(45),
    user_agent  TEXT,
    message     TEXT,
    request_id  UUID,
    source      VARCHAR(64),
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

-- Daily partitions — create one per day of retention window
-- Example: last 7 days (extend as needed)
CREATE TABLE web_logs_2026_04_15 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-15') TO ('2026-04-16');
CREATE TABLE web_logs_2026_04_16 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-16') TO ('2026-04-17');
CREATE TABLE web_logs_2026_04_17 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-17') TO ('2026-04-18');
CREATE TABLE web_logs_2026_04_18 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-18') TO ('2026-04-19');
CREATE TABLE web_logs_2026_04_19 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-19') TO ('2026-04-20');
CREATE TABLE web_logs_2026_04_20 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-20') TO ('2026-04-21');
CREATE TABLE web_logs_2026_04_21 PARTITION OF web_logs
    FOR VALUES FROM ('2026-04-21') TO ('2026-04-22');

-- Default partition catches anything outside the explicit ranges
CREATE TABLE web_logs_default PARTITION OF web_logs DEFAULT;


-- ────────────────────────────────────────────────────────────
--  3. INDEXES  (created on each partition automatically)
-- ────────────────────────────────────────────────────────────

-- Composite: timestamp + level — primary filter for most queries
CREATE INDEX idx_ts_level       ON web_logs (ts, level);

-- Endpoint performance queries
CREATE INDEX idx_path_status    ON web_logs (path, status_code);

-- Slowest-request queries
CREATE INDEX idx_duration_desc  ON web_logs (duration_ms DESC);

-- IP-based lookups / abuse detection
CREATE INDEX idx_ip             ON web_logs (ip_address);

-- Error-only partial index — tiny footprint, fast error dashboards
CREATE INDEX idx_errors_only    ON web_logs (ts, path)
    WHERE level = 'ERROR';


-- ────────────────────────────────────────────────────────────
--  4. MATERIALIZED VIEWS
-- ────────────────────────────────────────────────────────────

-- 4a. Hourly error counts + rates
CREATE MATERIALIZED VIEW vw_hourly_errors AS
SELECT
    DATE_TRUNC('hour', ts)                                          AS hour_bucket,
    COUNT(*)                                                        AS total_requests,
    SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END)               AS error_count,
    ROUND(
        100.0 * AVG(CASE WHEN level = 'ERROR' THEN 1.0 ELSE 0.0 END),
        2
    )                                                               AS error_rate_pct
FROM web_logs
GROUP BY hour_bucket
ORDER BY hour_bucket DESC
WITH DATA;

CREATE UNIQUE INDEX ON vw_hourly_errors (hour_bucket);


-- 4b. Endpoint performance: P50 / P95 / P99 latency
CREATE MATERIALIZED VIEW vw_endpoint_perf AS
SELECT
    path,
    COUNT(*)                                                        AS request_count,
    ROUND(AVG(duration_ms))                                         AS avg_ms,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms)      AS p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)      AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)      AS p99_ms,
    ROUND(
        100.0 * SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                                                               AS error_rate_pct
FROM web_logs
WHERE ts >= NOW() - INTERVAL '24 hours'
GROUP BY path
ORDER BY p95_ms DESC
WITH DATA;

CREATE UNIQUE INDEX ON vw_endpoint_perf (path);


-- 4c. Daily status code summary
CREATE MATERIALIZED VIEW vw_status_summary AS
SELECT
    DATE_TRUNC('day', ts)                                           AS day_bucket,
    FLOOR(status_code / 100) * 100                                  AS status_class,
    COUNT(*)                                                        AS request_count
FROM web_logs
GROUP BY day_bucket, status_class
ORDER BY day_bucket DESC, status_class
WITH DATA;

CREATE UNIQUE INDEX ON vw_status_summary (day_bucket, status_class);


-- ────────────────────────────────────────────────────────────
--  5. REFRESH FUNCTION  (call on a schedule, e.g. pg_cron)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION refresh_log_views()
RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_hourly_errors;
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_endpoint_perf;
    REFRESH MATERIALIZED VIEW CONCURRENTLY vw_status_summary;
END;
$$;

-- Schedule with pg_cron (run every 5 minutes):
-- SELECT cron.schedule('refresh-log-views', '*/5 * * * *', 'SELECT refresh_log_views()');


-- ────────────────────────────────────────────────────────────
--  6. UTILITY: auto-create next day's partition
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION create_partition_for_date(p_date DATE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    tbl_name TEXT;
    start_dt DATE := p_date;
    end_dt   DATE := p_date + INTERVAL '1 day';
BEGIN
    tbl_name := 'web_logs_' || TO_CHAR(start_dt, 'YYYY_MM_DD');
    EXECUTE FORMAT(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF web_logs
         FOR VALUES FROM (%L) TO (%L)',
        tbl_name, start_dt, end_dt
    );
    RAISE NOTICE 'Partition % created', tbl_name;
END;
$$;

-- Example: create tomorrow's partition
-- SELECT create_partition_for_date(CURRENT_DATE + 1);
