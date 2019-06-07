SELECT derived_table1.calculated_category_id, derived_table1.calculated_category_name, derived_table1.salary_bracket_id, derived_table1.average, "max"(derived_table1.spend) AS p_0, "max"(derived_table1.p_90) AS p_10, "max"(derived_table1.p_70) AS p_30, "max"(derived_table1.p_60) AS p_40, "max"(derived_table1.q2) AS p_50, "max"(derived_table1.p_40) AS p_60, "max"(derived_table1.p_30) AS p_70, "max"(derived_table1.p_20) AS p_80, "max"(derived_table1.p_10) AS p_90, min(derived_table1.spend) AS p_100
   FROM ( SELECT t.calculated_category_id, t.calculated_category_name, cb.salary_bracket_id, t.customer_id, sum(t.amount) AS spend, avg(sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS average, pg_catalog.percentile_cont(0.5) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS q2, pg_catalog.percentile_cont(0.1) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_10, pg_catalog.percentile_cont(0.2) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_20, pg_catalog.percentile_cont(0.3) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_30, pg_catalog.percentile_cont(0.4) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_40, pg_catalog.percentile_cont(0.6) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_60, pg_catalog.percentile_cont(0.7) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_70, pg_catalog.percentile_cont(0.8) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_80, pg_catalog.percentile_cont(0.9) WITHIN GROUP(
          ORDER BY sum(t.amount))
          OVER(
          PARTITION BY t.calculated_category_name, cb.salary_bracket_id) AS p_90
           FROM transactions t
      JOIN ( SELECT a_1.customer_id, pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                  ORDER BY a_1.total_amount) AS median_monthly_net_income,
                        CASE
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) >= 0::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 9285::numeric THEN 1
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 9285::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 17141::numeric THEN 2
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 17141::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 24316::numeric THEN 3
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 24316::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 30980::numeric THEN 4
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 30980::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 37269::numeric THEN 5
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 37269::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 43349::numeric THEN 6
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 43349::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 55149::numeric THEN 7
                            WHEN pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) > 55149::numeric AND pg_catalog.percentile_cont(0.5) WITHIN GROUP(
                      ORDER BY a_1.total_amount) <= 95449::numeric THEN 8
                            ELSE 9
                        END AS salary_bracket_id
                   FROM ( SELECT DISTINCT t.customer_id, sum(t.amount)
                          OVER(
                          PARTITION BY t.customer_id, date_trunc('month'::character varying::text, t.transaction_date)
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_amount, t.account_class
                           FROM transactions t
                          WHERE t.amount > 0::numeric::numeric(20,2) AND t.account_class::text = 'Bank'::character varying::text) a_1
                  GROUP BY a_1.customer_id) cb ON cb.customer_id::text = t.customer_id::text
   JOIN categories c ON c.id::text = t.calculated_category_id::text
  WHERE t.amount < 0::numeric(20,2) AND t.transaction_date >= date_add('day'::text, -31::bigint, date_trunc('month'::text, 'now'::text::date::timestamp without time zone)) AND t.transaction_date < date_trunc('day'::text, 'now'::text::date::timestamp without time zone)
  GROUP BY t.calculated_category_id, t.calculated_category_name, cb.salary_bracket_id, t.customer_id) derived_table1
  GROUP BY derived_table1.calculated_category_id, derived_table1.calculated_category_name, derived_table1.salary_bracket_id, derived_table1.average
  ORDER BY derived_table1.salary_bracket_id;
