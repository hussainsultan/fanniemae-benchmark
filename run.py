import datetime
import json
import platform
import time
import warnings
from pathlib import Path

import click
import duckdb
import ibis
import psutil
from jinja2 import Template
from memory_profiler import memory_usage

warnings.filterwarnings("ignore")


def create_duckdb(datadir, threads=psutil.cpu_count(), profile="profile.txt"):
    db = ibis.duckdb.connect("mortgage.db")
    perf_path = datadir / "perf/*.parquet"
    acq_path = datadir / "acq/*.parquet"
    db.con.execute(f"CREATE OR REPLACE VIEW perf AS SELECT * FROM '{perf_path}'")
    db.con.execute(f"CREATE OR REPLACE VIEW acq AS SELECT * FROM '{acq_path}'")
    db.con.execute(f"PRAGMA threads={threads}")
    if profile:
        db.con.execute("PRAGMA enable_profiling")
        db.con.execute(f"PRAGMA profiling_output='{profile}'")
    return db


def summary_query(db):
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
    return summary


def execute(expr, db):
    result = db.execute(expr)
    return result


def platform_info():
    return {
        "machine": platform.machine(),
        "version": platform.version(),
        "platform": platform.platform(),
        "system": platform.system(),
        "cpu_count": psutil.cpu_count(),
        "memory": psutil.virtual_memory().total,
        "processor": platform.processor(),
    }


def collect_stats(db):
    return {
        "total_rows_RHS": db.con.execute("select count(*) from perf").fetchall()[0][0],
        "row_count_LHS": db.con.execute("select count(*) from acq").fetchall()[0][0],
    }


QUERIES = {"summary": summary_query}


@click.command()
@click.option("--threads", default=psutil.cpu_count())
@click.option("--datadir", default="data")
def main(datadir, threads):
    datadir = Path(datadir)
    db = create_duckdb(datadir, threads=threads, profile="profile.txt")
    stats = []
    for query in QUERIES:
        expression = QUERIES[query](db)
        start_time = time.time()
        mem = memory_usage(
            (
                execute,
                (
                    expression,
                    db,
                ),
            )
        )
        total_time = time.time() - start_time
        run_stats = {
            "name": query,
            "threads": threads,
            "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "total_time": total_time,
            "max_memory_usage": max(mem),
            "incremental_memory_usage": mem[-1] - mem[0],
        }
        stats.append(run_stats)
    data = {
        **platform_info(),
        "runs": stats,
        "threads": threads,
        "schema_info": collect_stats(db),
        "datadir": str(datadir),
    }
    print(json.dumps(data))


if __name__ == "__main__":
    main()
