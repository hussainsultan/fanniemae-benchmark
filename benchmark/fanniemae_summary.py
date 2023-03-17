import ibis
from ibis import _


def summary_query(db):
    perf = db.table("perf")
    acq = db.table("acq")
    acq = acq[acq.loan_id, acq.orig_date.name("year"), acq.borrower_credit_score]
    joined = acq.inner_join(perf, acq.loan_id == perf.loan_id)

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
            summary.year,
            summary.loan_age,
            summary["count(loan_id)"],
            summary.avg_credit_score,
            summary.upb_sum,
            summary.dollar_co,
        ]
    )
    return summary
