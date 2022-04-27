WITH performance AS (
    SELECT
        perf.loan_id,
        book.orig_date,
        perf.loan_age AS stmt_number,
        book.borrower_credit_score,
        perf.disposition_date,
        perf.current_actual_upb,
        SUBSTRING(orig_date, POSITION('/' in orig_date)) AS report_month,
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
        perf,
        (
            SELECT
                loan_id,
                borrower_credit_score,
                SUBSTRING(orig_date, POSITION( '/' in orig_date)) AS orig_date
            FROM acq
        ) AS book
    WHERE
        perf.loan_id = book.loan_id
)
SELECT
    agg.orig_date,
    agg.stmt_number,
    loans.loan_count,
    agg.avg_credit_score,
    agg.upb_sum,
    agg.dollar_bad
FROM
    (SELECT
        orig_date,
        stmt_number,
        AVG(borrower_credit_score) AS avg_credit_score,
        SUM(CASE
            WHEN disposition_date IS NOT NULL THEN current_actual_upb
            ELSE 0
            END) AS dollar_bad,
        SUM(current_actual_upb) AS upb_sum
        FROM
            performance
        WHERE
            stmt_number >= 1
        GROUP BY orig_date,
                 stmt_number) AS agg,
    (SELECT
        SUBSTRING(orig_date, POSITION( '/' in orig_date)) AS orig_year,
        COUNT(loan_id) AS loan_count
    FROM
        acq
    GROUP BY
        orig_year) AS loans
WHERE
    agg.orig_date = loans.orig_year;
