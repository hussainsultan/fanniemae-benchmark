from pathlib import Path

import duckdb
import ibis


def create_db():
    conn = duckdb.connect("mortgage.db")
    conn.execute("CREATE VIEW perf AS SELECT * FROM 'data/perf.parquet'")
    conn.execute("CREATE VIEW acq AS SELECT * FROM 'data/acq.parquet'")
    conn.close()


def main():
    if not Path("mortgage.db").is_file():
        create_db()
    db = ibis.duckdb.connect("mortgage.db")
    perf = db.table("perf")
    acq = db.table("acq")
    acq = acq[
        acq.loan_id, acq.orig_date.split("/")[1].name("year"), acq.borrower_credit_score
    ]
    joined = acq.inner_join(perf, acq.loan_id == perf.loan_id)
    dq30 = (
        ibis.case()
        .when(perf.current_loan_delinquency_status > 1, 1)
        .else_(0)
        .end()
        .name("dq30")
    )

    chargeoffs = (
        ibis.case()
        .when(
            (perf.zero_balance_code.isin(["02", "03", "09", "15"]))
            & (perf.disposition_date.notnull()),
            1,
        )
        .else_(0)
        .end()
        .name("charegoffs")
    )

    dollar_co = (
        perf.zero_balance_code.isin(["02", "03", "09", "15"])
        & (perf.disposition_date.notnull())
    ).ifelse(perf.current_actual_upb, 0)
    loans = joined.mutate(chargeoffs=chargeoffs, dollar_co=dollar_co).projection(
        [
            perf.loan_id,
            # dq30,
            chargeoffs,
            dollar_co.name("dollar_co"),
            perf.loan_age,
            perf.current_actual_upb,
            acq.year,
            acq.borrower_credit_score,
        ]
    )

    summary = (
        loans[loans.loan_age > 0]
        .groupby([loans.year, loans.loan_age])
        .aggregate(
            co_count=lambda x: x.charegoffs.cast("int64").sum(),
            dollar_co=lambda x: x.dollar_co.sum(),
            avg_credit_score=lambda x: x.borrower_credit_score.mean(),
            upb_sum=lambda x: x.current_actual_upb.sum(),
        )
    )

    acq_agg = acq.groupby([acq.year]).loan_id.count()

    summary = summary.inner_join(acq_agg, acq_agg.year == summary.year)
    summary = summary.projection(
        [
            summary.year_x,
            summary.loan_age,
            summary["count(loan_id)"],
            summary.avg_credit_score,
            summary.upb_sum,
            summary.dollar_co,
        ]
    )
    print(summary.compile().compile(compile_kwargs={"literal_binds": True}))
    print(summary.execute().head())


if __name__ == "__main__":
    main()
