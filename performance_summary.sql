WITH performance AS (
    SELECT
        perf.loan_id,
        book.orig_date,
        first_statements.book_month,
        perf.loan_age AS stmt_number,
        book.borrower_credit_score,
        perf.disposition_date,
        perf.current_actual_upb,
        strftime(
            strptime(perf.monthly_reporting_period, '%Y-%d-%m'), '%Y-%m'
        ) AS report_month,
        CASE
            WHEN perf.current_loan_delinquency_status > 1 THEN 1
            ELSE 0
        END AS dq30,
        CASE
            WHEN perf.current_loan_delinquency_status > 3 THEN 1
            ELSE 0
        END AS dq90,
        CASE
            WHEN perf.current_loan_delinquency_status > 6 THEN 1
            ELSE 0
        END AS dq180,
        CASE
            WHEN perf.zero_balance_code = '02' THEN 1
            WHEN perf.zero_balance_code = '03' THEN 1
            WHEN perf.zero_balance_code = '09' THEN 1
            WHEN perf.zero_balance_code = '15' THEN 1
            ELSE 0
        END AS bad
    FROM
        {{ perf|tojson }} AS perf,
        (
            SELECT
                loan_id,
                strftime(
                    strptime(monthly_reporting_period, '%Y-%d-%m'), '%Y-%m'
                ) AS book_month
            FROM {{ perf|tojson }} WHERE loan_age = 0
        ) AS first_statements,
        (
            SELECT
                loan_id,
                borrower_credit_score,
                str_split(orig_date, '/')[1] AS orig_date
            FROM {{ acq|tojson }}
        ) AS book
    WHERE
        perf.loan_id = first_statements.loan_id
        AND first_statements.loan_id = book.loan_id
),

acquisition AS (
    SELECT
        str_split(orig_date, '/')[1] AS orig_year,
        count(loan_id) AS loan_count
    FROM
        {{ acq|tojson }}
    GROUP BY
        orig_year
)

SELECT
    agg.orig_date,
    agg.stmt_number,
    loans.loan_count,
    agg.avg_credit_score,
    agg.dq30_count,
    agg.dq90_count,
    agg.dq180_count,
    agg.upb_sum,
    agg.bad_cnt,
    agg.dollar_bad
FROM
    (SELECT
        orig_date,
	stmt_number,
        avg(borrower_credit_score) AS avg_credit_score,
        sum(dq30) AS dq30_count,
        sum(dq90) AS dq90_count,
        sum(dq180) AS dq180_count,
        sum(CASE
            WHEN disposition_date IS NOT NULL THEN bad
            ELSE 0
            END) AS bad_cnt,
        sum(CASE
            WHEN disposition_date IS NOT NULL THEN current_actual_upb
            ELSE 0
            END) AS dollar_bad,
        sum(current_actual_upb) AS upb_sum
        FROM
            performance
        WHERE
            stmt_number >= 1
        GROUP BY orig_date, 
                 stmt_number) AS agg,
    (SELECT
        orig_year,
        loan_count
        FROM acquisition) AS loans
WHERE
    agg.orig_date = loans.orig_year;
