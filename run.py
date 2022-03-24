import time
import tracemalloc
import warnings
from pathlib import Path

import duckdb
import ibis
from memory_profiler import memory_usage
from rich import print
from rich.console import Console
from rich.markdown import Markdown
from rich.syntax import Syntax

warnings.filterwarnings("ignore")


def create_db():
    conn = duckdb.connect("mortgage.db")
    conn.execute("CREATE VIEW perf AS SELECT * FROM 'data/perf.parquet'")
    conn.execute("CREATE VIEW acq AS SELECT * FROM 'data/acq.parquet'")
    conn.close()


def main():
    console = Console()
    if not Path("mortgage.db").is_file():
        create_db()
    db = ibis.duckdb.connect("mortgage.db")
    perf = db.table("perf")
    acq = db.table("acq")
    acq = acq[
        acq.loan_id, acq.orig_date.split("/")[1].name("year"), acq.borrower_credit_score
    ]
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
            summary.year_x,
            summary.loan_age,
            summary["count(loan_id)"],
            summary.avg_credit_score,
            summary.upb_sum,
            summary.dollar_co,
        ]
    )
    sql = summary.compile().compile(compile_kwargs={"literal_binds": True})

    syntax = Syntax(str(sql), "sql")
    console.print(syntax)
    tracemalloc.start()
    start_time = time.time()
    result = summary.execute()
    total_time = time.time() - start_time
    snapshot = tracemalloc.get_traced_memory()
    console.print(f"Peak memory usage: {snapshot[1]} kb")
    console.print(f"Total Time: {total_time:0.2f} seconds")


if __name__ == "__main__":
    main()
