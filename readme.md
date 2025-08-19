total_alert_query =
       SELECT COUNT(*) AS total_alerts
       FROM alert
       WHERE tenant_id = %s



    2.for document related query...

   document_expired_query =
       SELECT COUNT(document_id) AS expired_count
       FROM verification_documents
       WHERE tenant_id = %s
           AND document_status = 'ID Expired'
           AND document_expiry_date <= CURRENT_DATE;



    3.for customer risk score related query....high_risk_customers_query =
       SELECT COUNT(*) AS total_high_risk_customers
FROM (
    SELECT
        cii.tenant_id,
        'individual' AS customer_type,
        cii.id::text AS customer_id
    FROM customer_individual_identification cii
    WHERE cii.status <> 'Delete'
        AND cii.tenant_id = '4f12a90a-75e5-4ca6-815c-e467cb36b83d'

    UNION ALL

    SELECT
        cei.tenant_id,
        'entity' AS customer_type,
        cei.id::text AS customer_id
    FROM customer_entity_identification cei
    WHERE cei.status <> 'Delete'
        AND cei.tenant_id = '4f12a90a-75e5-4ca6-815c-e467cb36b83d'
) temp

JOIN (

    SELECT DISTINCT ON (cs.customer_id)
        cs.customer_id,
        cs.customer_score_id,
        cs.tenant_id,
        csrr.final_risk_score::integer AS final_risk_score_int,
        csrr.risk_rate::integer AS risk_rate_int,
        csrr.customer_type,
        csrr.created_at AS score_created_at
    FROM customer_score cs
    JOIN customer_score_risk_rate csrr
        ON csrr.customer_score_id = cs.customer_score_id
    ORDER BY cs.customer_id, csrr.created_at DESC
) cs_data
ON cs_data.customer_id = temp.customer_id

JOIN tenant_overall_risk_rates_settings tors_final
ON tors_final.tenant_id = cs_data.tenant_id
    AND tors_final.customer_type = cs_data.customer_type
    AND tors_final.risk_rate = cs_data.risk_rate_int
WHERE tors_final.default_risk_label IN ('H1', 'H2', 'H3');


4.for periodic review related query....period_review_due_query =
       WITH valid_customers AS (

   SELECT
       cii.id::text AS customer_id,
       'individual' AS customer_type,
       cii.tenant_id
   FROM customer_individual_identification cii
   WHERE cii.status <> 'Delete'::customer_individual_identification_status_enum
   AND cii.tenant_id = %s
    UNION ALL

    SELECT
       cei.id::text AS customer_id,
       'entity' AS customer_type,
       cei.tenant_id
   FROM customer_entity_identification cei
   WHERE cei.status <> 'Delete'::customer_entity_identification_status_enum
   AND cei.tenant_id = %s
),
ranked_scores AS (
   SELECT
       cs.customer_id,
       cs.customer_type,
       cs.tenant_id,
       csrr.periodic_review_due,
       ROW_NUMBER() OVER (PARTITION BY cs.customer_id ORDER BY csrr.created_at DESC) AS row_num
   FROM customer_score cs
   JOIN customer_score_risk_rate csrr
       ON cs.customer_score_id = csrr.customer_score_id
   WHERE cs.tenant_id = %s
),
eligible_scores AS (
   SELECT *
   FROM ranked_scores
   WHERE row_num = 1
   AND periodic_review_due::date <= CURRENT_DATE
)
SELECT COUNT(DISTINCT es.customer_id) AS total_periodic_review_due_count
FROM eligible_scores es
JOIN valid_customers vc
   ON es.customer_id = vc.customer_id
   AND es.customer_type = vc.customer_type
   AND es.tenant_id = vc.tenant_id;


5. for transaction monitor overview status related query....transaction_monitor_overview_query =
       SELECT
           TO_CHAR(DATE_TRUNC('month', t.flex_comply_date), 'Mon') AS month,
           EXTRACT(MONTH FROM t.flex_comply_date) AS month_num,
           pa.final_common_alert_result,
           COUNT(*) AS alert_count
       FROM
           transaction t
       JOIN
           transaction_monitoring_parent_alert pa ON t.parent_alert_id = pa.id
       WHERE
           t.flex_comply_date BETWEEN %s AND %s
           AND pa.final_common_alert_result IS NOT NULL
           AND t.tenant_id = %s
       GROUP BY
           TO_CHAR(DATE_TRUNC('month', t.flex_comply_date), 'Mon'),
           EXTRACT(MONTH FROM t.flex_comply_date),
           pa.final_common_alert_result
       ORDER BY
           EXTRACT(MONTH FROM t.flex_comply_date),
           pa.final_common_alert_result;


    6. for alert overview realted query.....alert_overview_query =
       WITH selected_categories AS (
           SELECT 'Ongoing Monitoring Alert' AS category_name
           UNION ALL
           SELECT 'Onboarding Alert'
           UNION ALL
           SELECT 'Periodic Alert'
       ),
       valid_categories AS (
           SELECT
               id,
               category_name
           FROM category
           WHERE
               tenant_id = %s
               AND category_name IN ('Ongoing Monitoring Alert', 'Onboarding Alert', 'Periodic Alert')
       ),
       alert_counts AS (
           SELECT
               vc.category_name,
               COUNT(a.id) AS alert_count
           FROM alert a
           JOIN valid_categories vc ON a.category_id = vc.id
           WHERE
               a.tenant_id = %s
               AND a.deleted_date IS NULL
               AND a.created_date BETWEEN %s AND %s
           GROUP BY vc.category_name
       )
       SELECT
           sc.category_name,
           COALESCE(ac.alert_count, 0) AS alert_count
       FROM
           selected_categories sc
       LEFT JOIN
           alert_counts ac ON sc.category_name = ac.category_name
       ORDER BY
           sc.category_name



  7. for regulatory reports related query...regulatory_report_filed_over_time =SELECT
  TO_CHAR(DATE_TRUNC('month', r.reported_date), 'Mon-YY') AS month,
  DATE_TRUNC('month', r.reported_date) AS month_sort,
  REPLACE(rt.report_template_name, 'Fintrac - ', '') AS report_type,
  COUNT(*) AS count
FROM
  reports r
JOIN
  reports_template_master rt ON r.template_id = rt.template_id
WHERE
  r.tenant_id = %s
  AND r.reported_date BETWEEN %s AND %s
GROUP BY
  month,
  month_sort,
  report_type
ORDER BY
  month_sort;

  8. for risk category related query...risk_category_changes_to_high_risk_category_over_time =  WITH year_input AS (
                    SELECT DATE %s AS year_start  -- e.g., '2025-01-01'
                ),
                months AS (
                    SELECT generate_series(
                        year_start,
                        year_start + INTERVAL '11 months',
                        INTERVAL '1 month'
                    ) AS month
                    FROM year_input
                ),
                ranked_scores AS (
                    SELECT
                        cs.customer_id,
                        cs.tenant_id,
                        cs.customer_type,
                        csrr.risk_rate::integer AS risk_bucket,
                        csrr.created_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY cs.customer_id, DATE_TRUNC('month', csrr.created_date)
                            ORDER BY csrr.created_date DESC
                        ) AS rn,
                        LAG(csrr.risk_rate::integer) OVER (
                            PARTITION BY cs.customer_id
                            ORDER BY csrr.created_date
                        ) AS prev_risk_bucket
                    FROM customer_score cs
                    JOIN customer_score_risk_rate csrr
                        ON cs.customer_score_id = csrr.customer_score_id
                    JOIN year_input y ON TRUE
                    WHERE cs.tenant_id = %s
                    AND csrr.created_date >= y.year_start
                    AND csrr.created_date < y.year_start + INTERVAL '1 year'
                ),
                transitions AS (
                    SELECT
                        DATE_TRUNC('month', rs.created_date) AS month,
                        CASE
                            WHEN rs.prev_risk_bucket = 1 AND rs.risk_bucket IN (3, 4, 5) THEN 'Low to High'
                            WHEN rs.prev_risk_bucket = 2 AND rs.risk_bucket IN (3, 4, 5) THEN 'Medium to High'
                            ELSE NULL
                        END AS transition_type
                    FROM ranked_scores rs
                    WHERE rs.rn = 1
                ),
                filtered_transitions AS (
                    SELECT * FROM transitions WHERE transition_type IS NOT NULL
                ),
                aggregated AS (
                    SELECT
                        month,
                        COUNT(*) FILTER (WHERE transition_type = 'Low to High') AS low_to_high,
                        COUNT(*) FILTER (WHERE transition_type = 'Medium to High') AS medium_to_high
                    FROM filtered_transitions
                    GROUP BY month
                )
                SELECT
                    TO_CHAR(m.month, 'Mon-YY') AS period,
                    COALESCE(a.low_to_high, 0) AS "Low to High",
                    COALESCE(a.medium_to_high, 0) AS "Medium to High"
                FROM months m
                LEFT JOIN aggregated a ON m.month = a.month
                ORDER BY m.month;


    9. for transaction volume by high risk customer related query...transaction_volume_by_high_risk_customer=WITH selected_year AS (
                    SELECT %s::date AS start_date
                ),
                months AS (
                    SELECT
                        generate_series(
                            start_date,
                            start_date + INTERVAL '11 months',
                            INTERVAL '1 month'
                        ) AS month_start
                    FROM selected_year
                ),
                transaction_data AS (
                    SELECT
                        t.id,
                        t.customer_id,
                        t.transaction_date,
                        t.transaction_amount,
                        t.base_amount,
                        t.tenant_id,
                        DATE_TRUNC('month', t.transaction_date) AS txn_month,
                        COALESCE(torrs.display_name,
                                CASE
                                    WHEN LOWER(TRIM(t.customer_type)) = 'individual' THEN ci.risk_category
                                    WHEN LOWER(TRIM(t.customer_type)) = 'entity' THEN ce.risk_category
                                END) AS customer_risk_category
                    FROM transaction t
                    LEFT JOIN customer_identification_log ci
                        ON t.customer_log_id = ci.id::varchar AND LOWER(TRIM(t.customer_type)) = 'individual'
                    LEFT JOIN customer_identification_log ce
                        ON t.customer_log_id = ce.id AND LOWER(TRIM(t.customer_type)) = 'entity'
                    LEFT JOIN customer_score cs
                        ON t.customer_id = cs.customer_id
                    LEFT JOIN customer_score_risk_rate csrr
                        ON csrr.customer_score_id = cs.customer_score_id
                    LEFT JOIN tenant_overall_risk_rates_settings torrs
                        ON torrs.risk_rate::SMALLINT = csrr.risk_rate::SMALLINT
                        AND torrs.tenant_id = t.tenant_id
                    WHERE t.transaction_date IS NOT NULL
                        AND t.tenant_id = %s
                        AND DATE_TRUNC('month', t.transaction_date) >= (SELECT start_date FROM selected_year)
                        AND DATE_TRUNC('month', t.transaction_date) < (SELECT start_date + INTERVAL '1 year' FROM selected_year)
                        AND COALESCE(torrs.display_name,
                                    CASE
                                        WHEN LOWER(TRIM(t.customer_type)) = 'individual' THEN ci.risk_category
                                        WHEN LOWER(TRIM(t.customer_type)) = 'entity' THEN ce.risk_category
                                    END) IN ('HL1', 'HL2', 'HL3')
                ),
                aggregated AS (
                    SELECT
                        txn_month,
                        COUNT(id) AS transaction_count,
                        COUNT(DISTINCT customer_id) AS unique_customers,
                        SUM(transaction_amount::numeric) AS total_transaction_amount,
                        SUM(base_amount::numeric) AS total_base_amount
                    FROM transaction_data
                    GROUP BY txn_month
                )
                SELECT
                    TO_CHAR(m.month_start, 'Mon-YY') AS month,
                    COALESCE(a.transaction_count, 0) AS transaction_count,
                    COALESCE(a.unique_customers, 0) AS unique_customers,
                    ROUND(COALESCE(a.total_transaction_amount, 0) / 1000.0, 2) AS total_transaction_amount_thousands,
                    ROUND(COALESCE(a.total_base_amount, 0) / 1000.0, 2) AS total_base_amount_thousands
                FROM months m
                LEFT JOIN aggregated a ON m.month_start = a.txn_month
                ORDER BY m.month_start;


    10.value of transaction risk categories query= WITH selected_month_input AS (
                SELECT DATE %s AS month_start  -- e.g., '2025-03-01'
            ),
            categorized_transactions AS (
                SELECT
                    CASE
                        WHEN ap.priority_risk_category ILIKE 'High' THEN 'High Risk TXN Value'
                        WHEN ap.priority_risk_category ILIKE 'Medium' THEN 'Medium Risk TXN Value'
                        WHEN ap.priority_risk_category ILIKE 'Low' THEN 'Low Risk TXN Value'
                        ELSE 'Unknown'
                    END AS risk_group,
                    t.transaction_amount,
                    t.base_amount
                FROM alert_prioritisation ap
                JOIN transaction_monitoring_parent_alert tmpa
                    ON ap.reference_id::int = tmpa.id
                JOIN transaction t
                    ON tmpa.transaction_id = t.id
                JOIN selected_month_input sm ON TRUE
                WHERE ap.tenant_id = %s
                AND t.transaction_date >= sm.month_start
                AND t.transaction_date < sm.month_start + INTERVAL '1 month'
            )
            SELECT
                risk_group,
                ROUND(SUM(transaction_amount), 2) AS total_transaction_amount,
                ROUND(SUM(base_amount), 2) AS total_base_amount
            FROM categorized_transactions
            WHERE risk_group != 'Unknown'
            GROUP BY risk_group
            ORDER BY
                CASE
                    WHEN risk_group = 'High Risk TXN Value' THEN 1
                    WHEN risk_group = 'Medium Risk TXN Value' THEN 2
                    WHEN risk_group = 'Low Risk TXN Value' THEN 3
                    ELSE 4
                END;

       11 . transaction in high risk countries = WITH high_risk_countries AS (
                    SELECT UNNEST(ARRAY[
                        'Democratic People''s Republic of Korea', 'Iran', 'Myanmar', 'Algeria', 'Angola', 'Bolivia', 'Bulgaria', 'Burkina Faso',
                        'Cameroon', 'Côte d''Ivoire', 'Democratic Republic of Congo', 'Haiti', 'Kenya', 'Lao People''s Democratic Republic',
                        'Lebanon', 'Monaco', 'Mozambique', 'Namibia', 'Nepal', 'Nigeria', 'South Africa', 'South Sudan', 'Syria',
                        'Venezuela', 'Vietnam', 'Virgin Islands (UK)', 'Yemen'
                    ]) AS country_name
                ),

                counterparty_country AS (
                    SELECT
                        t.id AS transaction_id,
                        t.transaction_direction,
                        cp.counter_party_country AS country
                    FROM transaction t
                    JOIN counter_party cp ON t.counter_party_id = cp.id
                    WHERE t.tenant_id = %s
                    AND EXTRACT(YEAR FROM t.transaction_date) = %s
                )

                SELECT DISTINCT
                    cc.country
                FROM counterparty_country cc
                JOIN high_risk_countries hrc
                    ON cc.country ILIKE hrc.country_name
                WHERE cc.transaction_direction = %s;

    12.     transaction_involving_high_risk_country_query =
                WITH selected_year AS (
                    SELECT %s::date AS year_start
                ),
                months AS (
                    SELECT
                        TO_CHAR(month_start, 'Mon') AS month,
                        EXTRACT(MONTH FROM month_start)::int AS month_number,
                        month_start
                    FROM generate_series(
                        (SELECT year_start FROM selected_year),
                        (SELECT year_start + INTERVAL '11 months' FROM selected_year),
                        interval '1 month'
                    ) AS month_series(month_start)
                ),
                high_risk_country_list AS (
                    SELECT UNNEST(ARRAY[
                        'Democratic People''s Republic of Korea', 'Iran', 'Myanmar', 'Algeria', 'Angola', 'Bolivia', 'Bulgaria', 'Burkina Faso',
                        'Cameroon', 'Côte d''Ivoire', 'Democratic Republic of Congo', 'Haiti', 'Kenya', 'Lao People''s Democratic Republic',
                        'Lebanon', 'Monaco', 'Mozambique', 'Namibia', 'Nepal', 'Nigeria', 'South Africa', 'South Sudan', 'Syria',
                        'Venezuela', 'Vietnam', 'Virgin Islands (UK)', 'Yemen'
                    ]) AS high_risk_country
                ),
                -- Only Counterparty Side with direction filter
                counterparty_countries AS (
                    SELECT
                        t.id,
                        t.transaction_amount,
                        t.base_amount,
                        t.transaction_direction,
                        t.transaction_currency,
                        DATE_TRUNC('month', t.transaction_date) AS txn_month,
                        cp.counter_party_country AS country
                    FROM transaction t
                    JOIN counter_party cp ON cp.id = t.counter_party_id
                    WHERE t.tenant_id = %s
                        AND t.transaction_direction = %s  -- 👈 Direction filter here
                        AND t.transaction_date >= (SELECT year_start FROM selected_year)
                        AND t.transaction_date < (SELECT year_start + INTERVAL '1 year' FROM selected_year)
                ),
                filtered_high_risk_txns AS (
                    SELECT DISTINCT ON (id)
                        id,
                        cc.txn_month,
                        cc.base_amount,
                        cc.transaction_direction
                    FROM counterparty_countries cc
                    JOIN high_risk_country_list hrc
                        ON cc.country ILIKE hrc.high_risk_country
                ),
                agg AS (
                    SELECT
                        txn_month,
                        transaction_direction,
                        COUNT(*) AS transaction_count,
                        SUM(base_amount::NUMERIC) AS total_base_amounts,
                        ROUND(SUM(base_amount) / 10000.0, 2) AS transaction_base_value_millions
                    FROM filtered_high_risk_txns
                    GROUP BY txn_month, transaction_direction
                )
                SELECT
                    m.month,
                    m.month_number,
                    a.transaction_direction,
                    COALESCE(a.total_base_amounts, 0) AS total_base_amounts,
                    COALESCE(a.transaction_count, 0) AS transaction_count,
                    COALESCE(a.transaction_base_value_millions, 0.00) AS transaction_base_value_millions
                FROM months m
                LEFT JOIN agg a ON a.txn_month = m.month_start
                ORDER BY m.month_number;


        13. tm_rule_mapped_to_alert_status_query =
        SELECT
            COUNT(*) AS total_count,
            rule_code,
            fm_alert_status
        FROM transaction_monitoring_alert tma
        JOIN transaction_monitoring_parent_alert tmpa ON tmpa.id = tma.parent_alert_id
        JOIN transaction t ON t.parent_alert_id = tmpa.id
        WHERE
            tanent_id = %s
            AND convert_timezone_with_interval(
                t.flex_comply_date::TEXT,
                'Asia/Kolkata', 'Asia/Kolkata', false, false
            )::DATE BETWEEN %s::DATE AND %s::DATE
            AND fm_alert_status IN ('FM Alert Open', 'FM Alert Review')
        GROUP BY rule_code, fm_alert_status
        ORDER BY rule_code;


        14.for true positive screening matches related query..use this query..  tp_screening_matches_query =
                WITH selected_year AS (
                    SELECT %s::date AS year_start
                ),
                months AS (
                    SELECT
                        generate_series(
                            year_start,
                            year_start + interval '11 months',
                            interval '1 month'
                        ) AS month_start
                    FROM selected_year
                ),
                filtered_matches AS (
                    SELECT
                        created_date,
                        source_names
                    FROM screening_match
                    WHERE tenant_id = %s
                    AND screening_status IN ('True Positive Accepted', 'True Positive Rejected')
                    AND created_date >= (SELECT year_start FROM selected_year)
                    AND created_date < (SELECT year_start + interval '1 year' FROM selected_year)
                )
                SELECT
                    TO_CHAR(m.month_start, 'Mon-YY') AS period,
                    COUNT(CASE
                        WHEN ',' || LOWER(f.source_names) || ',' LIKE '%%,peps,%%' THEN 1
                    END) AS "No of PEP Matches",
                    COUNT(CASE
                        WHEN ',' || LOWER(f.source_names) || ',' LIKE '%%,sanctions,%%' THEN 1
                    END) AS "No of Sanctions Matches"
                FROM months m
                LEFT JOIN filtered_matches f
                    ON date_trunc('month', f.created_date) = m.month_start
                GROUP BY m.month_start
                ORDER BY m.month_start;

        15. for regulatory report filed over time related query....    regulatory_reporting_value_query =
                WITH selected_year AS (
                    SELECT %s::int AS year
                ),
                months AS (
                    SELECT
                        TO_CHAR(month_start, 'Mon-YY') AS period,
                        DATE_TRUNC('month', month_start) AS month_start,
                        EXTRACT(MONTH FROM month_start)::int AS month_number
                    FROM generate_series(
                        make_date((SELECT year FROM selected_year), 1, 1),
                        make_date((SELECT year FROM selected_year), 12, 1),
                        interval '1 month'
                    ) AS month_series(month_start)
                ),
                prefixes AS (
                    SELECT DISTINCT template_id, prefix FROM reports_template_master
                ),
                report_counts AS (
                    SELECT
                        DATE_TRUNC('month', r.reported_date) AS month_start,
                        rt.prefix,
                        COUNT(*) AS report_count
                    FROM reports r
                    JOIN prefixes rt ON r.template_id = rt.template_id
                    WHERE r.tenant_id = %s
                    AND EXTRACT(YEAR FROM r.reported_date) = (SELECT year FROM selected_year)
                    GROUP BY DATE_TRUNC('month', r.reported_date), rt.prefix
                ),
                all_combinations AS (
                    SELECT m.month_start, m.period, m.month_number, p.prefix
                    FROM months m
                    CROSS JOIN (SELECT DISTINCT prefix FROM prefixes) p
                )
                SELECT
                    ac.period,
                    ac.prefix,
                    COALESCE(rc.report_count, 0) AS report_count
                FROM all_combinations ac
                LEFT JOIN report_counts rc
                    ON rc.month_start = ac.month_start AND rc.prefix = ac.prefix
                ORDER BY ac.month_number, ac.prefix;

        16.for high risk transactions risk category...risk_trxn_query =
                WITH selected_month_input AS (
                    SELECT DATE %s AS month_start  -- e.g., '2025-01-01'
                ),
                categorized_transactions AS (
                    SELECT
                        CASE
                            WHEN ap.priority_risk_category = 'High' THEN 'High Risk Transaction Value percentage'
                            ELSE 'Other Risk Category Transaction Values'
                        END AS risk_group,
                        t.transaction_amount,
                        t.base_amount
                    FROM alert_prioritisation ap
                    JOIN transaction_monitoring_parent_alert tmpa
                        ON ap.reference_id::int = tmpa.id
                    JOIN transaction t
                        ON tmpa.transaction_id = t.id
                    JOIN selected_month_input sm ON TRUE
                    WHERE ap.tenant_id = %s
                    AND t.transaction_date >= sm.month_start
                    AND t.transaction_date < sm.month_start + INTERVAL '1 month'
                )
                SELECT
                    risk_group,
                    SUM(base_amount) AS total_base_amount,
                    ROUND(SUM(base_amount) * 100.0 / SUM(SUM(base_amount)) OVER (), 2) AS base_amt_percentage
                FROM categorized_transactions
                GROUP BY risk_group;

        17.for tm rule mapped to alert statuses (open,review) related query.....tm_rule_mapped_to_alert_status_query =
        SELECT
            COUNT(*) AS total_count,
            rule_code,
            fm_alert_status
        FROM transaction_monitoring_alert tma
        JOIN transaction_monitoring_parent_alert tmpa ON tmpa.id = tma.parent_alert_id
        JOIN transaction t ON t.parent_alert_id = tmpa.id
        WHERE
            tanent_id = %s
            AND convert_timezone_with_interval(
                t.flex_comply_date::TEXT,
                'Asia/Kolkata', 'Asia/Kolkata', false, false
            )::DATE BETWEEN %s::DATE AND %s::DATE
            AND fm_alert_status IN ('FM Alert Open', 'FM Alert Review')
        GROUP BY rule_code, fm_alert_status
        ORDER BY rule_code;

    18.for no of alerts triggered monthwise...no_of_alerts_triggered_monthwise_query =
            WITH selected_month AS (
                SELECT TO_DATE(%s, 'Mon-YY') AS start_date
            ),
            alerts_filtered AS (
                SELECT
                    TO_CHAR(DATE_TRUNC('month', convert_timezone_with_interval(
                        t.flex_comply_date::TEXT,
                        'Asia/Kolkata', 'Asia/Kolkata',
                        false, false
                    )::timestamp), 'Mon-YY') AS period,
                    tma.rule_code
                FROM transaction_monitoring_alert tma
                JOIN transaction_monitoring_parent_alert tmpa
                    ON tmpa.id = tma.parent_alert_id
                JOIN transaction t
                    ON t.parent_alert_id = tmpa.id
                WHERE tmpa.tanent_id = %s
                AND DATE_TRUNC('month', convert_timezone_with_interval(
                        t.flex_comply_date::TEXT,
                        'Asia/Kolkata', 'Asia/Kolkata',
                        false, false
                    )::timestamp) = (SELECT start_date FROM selected_month)
            )
            SELECT
                rule_code,
                COUNT(*) AS total_count
            FROM alerts_filtered
            GROUP BY rule_code
            ORDER BY rule_code;

