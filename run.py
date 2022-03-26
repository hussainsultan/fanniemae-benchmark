import time
import warnings
from pathlib import Path

import click
import duckdb
import ibis
from jinja2 import Template
from memory_profiler import profile
from rich.console import Console
from rich.markdown import Markdown
from rich.syntax import Syntax

warnings.filterwarnings("ignore")


def create_db(datadir):
    conn = duckdb.connect("mortgage.db")
    perf_path = datadir / "perf/*.parquet"
    acq_path = datadir / "acq/*.parquet"
    conn.execute(f"CREATE VIEW perf AS SELECT * FROM '{perf_path}'")
    conn.execute(f"CREATE VIEW acq AS SELECT * FROM '{acq_path}'")
    conn.close()


def generate_summary_expr():
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
    del db
    return summary


def generate_summary_sql(datadir):
    with open("performance_summary.sql") as f:
        template = Template(f.read())
    return template.render(
        perf=str(datadir / "perf/*.parquet"), acq=str(datadir / "acq/*.parquet")
    )


@profile
def execute(expr):
    conn = duckdb.connect("mortgage.db")
    result = conn.execute(str(expr)).fetchdf()
    conn.close()
    return result


@click.command()
@click.option("--datadir", default="data")
@click.option("--mode", default="sql")
def main(mode, datadir):
    datadir = Path(datadir)
    console = Console()
    if not Path("mortgage.db").is_file():
        create_db(datadir)
    if mode == "ibis":
        summary = generate_summary_expr()
        sql = summary.compile().compile(compile_kwargs={"literal_binds": True})
    else:
        sql = generate_summary_sql(datadir)

    syntax = Syntax(str(sql), "sql")

    console.print(syntax)
    console.print("Executing")
    start_time = time.time()
    result = execute(sql)
    total_time = time.time() - start_time
    console.print(f"Total time: {total_time:0.2f} seconds")


if __name__ == "__main__":
    main()
