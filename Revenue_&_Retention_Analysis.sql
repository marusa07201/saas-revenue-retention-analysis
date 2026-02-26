WITH payments AS (
    SELECT
        gp.user_id,
        gp.game_name,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model,
        DATE_TRUNC('month', gp.payment_date)::date AS payment_month,
        SUM(gp.revenue_amount_usd) AS total_revenue
    FROM project.games_payments gp
    JOIN project.games_paid_users gpu
        ON gp.user_id = gpu.user_id
    GROUP BY
        gp.user_id,
        gp.game_name,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model,
        DATE_TRUNC('month', gp.payment_date)
),
user_metrics AS (
    SELECT
        *,
        LAG(payment_month) OVER (
            PARTITION BY user_id, game_name
            ORDER BY payment_month
        ) AS previous_paid_month,
        LEAD(payment_month) OVER (
            PARTITION BY user_id, game_name
            ORDER BY payment_month
        ) AS next_paid_month,
        LAG(total_revenue) OVER (
            PARTITION BY user_id, game_name
            ORDER BY payment_month
        ) AS previous_paid_revenue,
        payment_month + INTERVAL '1 month' AS next_calendar_month,
        payment_month - INTERVAL '1 month' AS previous_calendar_month
    FROM payments
),
final AS (
    SELECT
        payment_month,
        game_name,
        language,
        age,
        has_older_device_model,
        COUNT(DISTINCT user_id) AS paid_users,
        SUM(total_revenue) AS mrr,
        SUM(CASE WHEN previous_paid_month IS NULL THEN 1 ELSE 0 END) AS new_paid_users,
        SUM(CASE WHEN previous_paid_month IS NULL THEN total_revenue ELSE 0 END) AS new_mrr,
        SUM(CASE 
            WHEN next_paid_month IS NULL
              OR next_paid_month != next_calendar_month
            THEN 1 ELSE 0 END) AS churned_users,
        SUM(CASE 
            WHEN next_paid_month IS NULL
              OR next_paid_month != next_calendar_month
            THEN total_revenue ELSE 0 END) AS churned_revenue,
        SUM(CASE 
            WHEN previous_paid_month = previous_calendar_month
             AND total_revenue > previous_paid_revenue
            THEN total_revenue - previous_paid_revenue
            ELSE 0 END) AS expansion_mrr,
        SUM(CASE 
            WHEN previous_paid_month = previous_calendar_month
             AND total_revenue < previous_paid_revenue
            THEN total_revenue - previous_paid_revenue
            ELSE 0 END) AS contraction_mrr,
        SUM(CASE
            WHEN previous_paid_month IS NOT NULL
             AND previous_paid_month != previous_calendar_month
            THEN total_revenue
            ELSE 0 END) AS back_from_churn_revenue
    FROM user_metrics
    GROUP BY
        payment_month,
        game_name,
        language,
        age,
        has_older_device_model
)
SELECT *
FROM final
ORDER BY payment_month, game_name, language, age;
