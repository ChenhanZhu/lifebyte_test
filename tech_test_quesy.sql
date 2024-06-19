WITH date_range AS (
    SELECT generate_series(
        '2020-06-01'::date,
        '2020-09-30'::date,
        '1 day'::interval
    )::date AS dt_report
),
user_accounts AS (
    SELECT DISTINCT login_hash
    FROM login_hash
    WHERE enable = 1
),
report_data AS (
    SELECT
        dr.dt_report,
        ua.login_hash,
        t.server_hash,
        t.symbol,
        us.currency,
        COALESCE(SUM(CASE WHEN t.open_time >= dr.dt_report - INTERVAL '6 days' THEN t.volume ELSE 0 END), 0) AS sum_volume_prev_7d,
        COALESCE(SUM(t.volume) OVER (PARTITION BY t.symbol, ua.login_hash, t.server_hash ORDER BY t.open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS sum_volume_prev_all,
        DENSE_RANK() OVER (PARTITION BY ua.login_hash, t.symbol ORDER BY SUM(t.volume) DESC) AS rank_volume_symbol_prev_7d,
        DENSE_RANK() OVER (PARTITION BY ua.login_hash ORDER BY COUNT(*) DESC) AS rank_count_prev_7d,
        COALESCE(SUM(CASE WHEN EXTRACT(YEAR FROM t.open_time) = 2020 AND EXTRACT(MONTH FROM t.open_time) = 8 THEN t.volume ELSE 0 END), 0) AS sum_volume_2020_08,
        MIN(t.open_time) AS date_first_trade,
        ROW_NUMBER() OVER (ORDER BY dr.dt_report DESC, ua.login_hash, t.server_hash, t.symbol) AS row_number
    FROM
        date_range dr
    CROSS JOIN
        user_accounts ua
    JOIN
        users us ON ua.login_hash = us.login_hash
    LEFT JOIN
        trades t ON dr.dt_report >= t.open_time::date AND ua.login_hash = t.login_hash
    GROUP BY
        dr.dt_report, ua.login_hash, t.server_hash, t.symbol, us.currency, t.open_time, t.volume
)
SELECT
    ROW_NUMBER() OVER () AS id,
    rd.*
FROM
    report_data rd
ORDER BY
    row_number DESC;