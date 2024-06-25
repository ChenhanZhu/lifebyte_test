WITH date_range AS (
    SELECT generate_series(
        '2020-06-01'::date,
        '2020-09-30'::date,
        '1 day'::interval
    )::date AS dt_report
),

report_data AS (
    SELECT
		distinct
        dr2.dt_report,
        rwc.login_hash,
        rwc.server_hash,
        rwc.symbol,
        us.currency,
        rwc.sum_volume_prev_7d,
        rwc.sum_volume_prev_all,
	 	DENSE_RANK() OVER (PARTITION BY us.login_hash, rwc.symbol ORDER BY rwc.sum_volume_prev_7d DESC) AS rank_volume_symbol_prev_7d,
        DENSE_RANK() OVER (PARTITION BY us.login_hash ORDER BY rwc.cnt_volume_prev_7d DESC) AS rank_count_prev_7d,
        rwc.sum_volume_2020_08,
        rwc.date_first_trade

    FROM
	(
	Select dr.dt_report,
        us.login_hash,
        t.server_hash,
        t.symbol,
			(
		Select SUM(trades.volume)
		From trades
		Where trades.close_time >= dr.dt_report - INTERVAL '6 days'
		AND us.login_hash=trades.login_hash
		AND t.server_hash=trades.server_hash
		AND t.symbol=trades.symbol
		group by trades.login_hash, trades.server_hash, trades.symbol
	)
	as sum_volume_prev_7d,
			(
		Select count(trades.volume)
		From trades
		Where trades.close_time >= dr.dt_report - INTERVAL '6 days'
		AND us.login_hash=trades.login_hash
		AND t.server_hash=trades.server_hash
		AND t.symbol=trades.symbol
		group by trades.login_hash, trades.server_hash, trades.symbol
	)
	as cnt_volume_prev_7d,

	(Select SUM(trades.volume)
		From trades
		Where trades.close_time::date <= dr.dt_report
		AND us.login_hash=trades.login_hash
		AND t.server_hash=trades.server_hash
		AND t.symbol=trades.symbol
		group by trades.login_hash, trades.server_hash, trades.symbol
	)
	as sum_volume_prev_all,


(
		Select SUM(trades.volume)
		From trades
		Where trades.close_time::date >= '2020-08-01' AND trades.close_time::date < '2020-09-01' AND trades.close_time <= dr.dt_report
		AND us.login_hash=trades.login_hash
		AND t.server_hash=trades.server_hash
		AND t.symbol=trades.symbol
		group by trades.login_hash, trades.server_hash, trades.symbol
	)
	as sum_volume_2020_08,
        MIN(t.close_time) AS date_first_trade

		From
	date_range dr
    CROSS JOIN
        users us
    LEFT JOIN
        trades t ON dr.dt_report = t.close_time::date AND us.login_hash = t.login_hash
	WHERE
		dr.dt_report IS NOT NULL
        AND us.login_hash IS NOT NULL
        AND t.server_hash IS NOT NULL
        AND t.symbol IS NOT NULL
		AND us.enable=1

	GROUP BY
        dr.dt_report, us.login_hash, t.server_hash, t.symbol
	) rwc
	JOIN
        users us ON rwc.login_hash = us.login_hash
	right join
		date_range dr2 on dr2.dt_report=rwc.dt_report

)
SELECT
	ROW_NUMBER() OVER(ORDER BY (SELECT 1) ASC) AS id,
    rd.*,
	ROW_NUMBER() OVER (ORDER BY rd.dt_report, rd.login_hash, rd.server_hash, rd.symbol) AS row_number
FROM
    report_data rd
ORDER BY
    rd.dt_report DESC, row_number DESC;
