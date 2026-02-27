WITH all_months AS (
    -- List of all months from March 2022 to December 2022
    SELECT generate_series(DATE '2022-03-01', DATE '2022-12-01', '1 month'::interval) AS month
),
user_first_payments AS (
    -- First payment month for each user
    SELECT user_id, MIN(DATE_TRUNC('month', payment_date)) AS first_payment_month
    FROM project.games_payments
    GROUP BY user_id
),
user_payments AS (
    -- Individual payments per user per month
    SELECT user_id, DATE_TRUNC('month', payment_date) AS payment_month, SUM(revenue_amount_usd) AS revenue
    FROM project.games_payments
    GROUP BY user_id, payment_month
),
user_all_months AS (
    -- All user-month combinations, including months with no payments
    SELECT u.user_id, am.month
    FROM 
        (SELECT DISTINCT user_id FROM project.games_paid_users) u
    CROSS JOIN all_months am
),
user_payments_with_lag AS (
    -- All user-month combinations with actual payments; LAG to retrieve previous month
    SELECT 
        uam.user_id,
        uam.month,
        up.payment_month,
        LAG(up.payment_month) OVER (PARTITION BY uam.user_id ORDER BY uam.month) AS prev_payment_month,
        COALESCE(up.revenue, 0) AS revenue -- Revenue for later use
    FROM 
        user_all_months uam
    LEFT JOIN 
        user_payments up ON uam.user_id = up.user_id AND uam.month = up.payment_month
),
user_grouped AS (
    -- Users + payment data to calculate metrics
    SELECT 
        am.month,
        u.language,
        u.age,
        COUNT(DISTINCT CASE WHEN up.user_id IS NOT NULL THEN up.user_id END) AS paid_users,
        COUNT(DISTINCT CASE WHEN ufp.first_payment_month = am.month THEN ufp.user_id END) AS new_users,
        COUNT(DISTINCT CASE WHEN ufp.first_payment_month < am.month AND up.user_id IS NOT NULL THEN ufp.user_id END) AS recurring_users,
        SUM(CASE WHEN ufp.first_payment_month < am.month THEN up.revenue ELSE 0 END) AS mrr, -- Revenue from recurring users
        SUM(CASE WHEN ufp.first_payment_month = am.month THEN up.revenue ELSE 0 END) AS new_mrr, -- Revenue from new users
        SUM(up.revenue) AS total_revenue -- Total revenue per month
    FROM 
        all_months am
    LEFT JOIN 
        user_first_payments ufp ON ufp.first_payment_month <= am.month
    LEFT JOIN 
        project.games_paid_users u ON ufp.user_id = u.user_id
    LEFT JOIN 
        user_payments up ON up.user_id = ufp.user_id AND up.payment_month = am.month
    GROUP BY 
        am.month, u.language, u.age
),
churned_users AS (
    -- Churned users: made a payment last month but not in the current month
    SELECT 
        u.user_id,
        u.language,
        u.age,
        uam.month AS churned_month,
        COALESCE(up.revenue, 0) AS churned_revenue -- Previous month revenue from churned users
    FROM 
        user_payments_with_lag uam
    LEFT JOIN 
        project.games_paid_users u ON uam.user_id = u.user_id
    LEFT JOIN 
        user_payments up ON up.user_id = uam.user_id AND up.payment_month = uam.prev_payment_month
    WHERE 
        uam.prev_payment_month IS NOT NULL -- User made a payment in the previous month
        AND uam.payment_month IS NULL -- User did not make a payment in the current month
),
expansion_mrr_calc AS (
    -- Expansion MRR
    SELECT 
        u.language,
        u.age,
        uam.month,
        SUM(CASE 
            WHEN up.revenue > prev_up.revenue 
            THEN up.revenue - COALESCE(prev_up.revenue, 0)
            ELSE 0
        END) AS expansion_mrr
    FROM 
        user_payments_with_lag uam
    LEFT JOIN 
        project.games_paid_users u ON uam.user_id = u.user_id
    LEFT JOIN 
        user_payments up ON up.user_id = uam.user_id AND up.payment_month = uam.month
    LEFT JOIN 
        LATERAL (
            SELECT revenue 
            FROM user_payments 
            WHERE user_id = uam.user_id AND payment_month = uam.prev_payment_month
            LIMIT 1
        ) AS prev_up ON true
    GROUP BY 
        u.language, u.age, uam.month
),
contraction_mrr_calc AS (
    -- Contraction MRR
    SELECT 
        u.language,
        u.age,
        uam.month,
        SUM(CASE 
            WHEN COALESCE(prev_up.revenue, 0) > COALESCE(up.revenue, 0)
            THEN COALESCE(prev_up.revenue, 0) - COALESCE(up.revenue, 0)
            ELSE 0
        END) AS contraction_mrr
    FROM 
        user_payments_with_lag uam
    LEFT JOIN 
        project.games_paid_users u ON uam.user_id = u.user_id
    LEFT JOIN 
        user_payments up ON up.user_id = uam.user_id AND up.payment_month = uam.month
    LEFT JOIN 
        LATERAL (
            SELECT revenue 
            FROM user_payments 
            WHERE user_id = uam.user_id AND payment_month = uam.prev_payment_month
            LIMIT 1
        ) AS prev_up ON true
    GROUP BY 
        u.language, u.age, uam.month
)
SELECT 
    fu.month,
    fu.language,
    fu.age,
    fu.paid_users,
    fu.new_users,
    fu.recurring_users,
    fu.mrr,
    fu.new_mrr,
    fu.total_revenue, 
    COUNT(DISTINCT cu.user_id) AS churned_users,
    SUM(cu.churned_revenue) AS churned_revenue, 
    COALESCE(em.expansion_mrr, 0) AS expansion_mrr,
    COALESCE(cm.contraction_mrr, 0) AS contraction_mrr
FROM 
    user_grouped fu
LEFT JOIN 
    churned_users cu ON fu.month = cu.churned_month 
                     AND fu.language = cu.language 
                     AND fu.age = cu.age
LEFT JOIN 
    (SELECT month, language, age, SUM(expansion_mrr) AS expansion_mrr 
     FROM expansion_mrr_calc 
     GROUP BY month, language, age) em ON fu.month = em.month 
                                         AND fu.language = em.language 
                                         AND fu.age = em.age
LEFT JOIN 
    (SELECT month, language, age, SUM(contraction_mrr) AS contraction_mrr 
     FROM contraction_mrr_calc 
     GROUP BY month, language, age) cm ON fu.month = cm.month 
                                         AND fu.language = cm.language 
                                         AND fu.age = cm.age
GROUP BY 
    fu.month, fu.language, fu.age, fu.paid_users, fu.new_users, fu.recurring_users, 
    fu.mrr, fu.new_mrr, fu.total_revenue, em.expansion_mrr, cm.contraction_mrr
ORDER BY 
    fu.month, fu.language, fu.age;
