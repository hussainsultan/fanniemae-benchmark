import datetime
import json
import os
import platform
import time
import timeit
import warnings
from pathlib import Path
from shutil import which

import click
import duckdb
import ibis
import pandas as pd
import psutil
from jinja2 import Template
from memory_profiler import memory_usage

from powercap_rapl import PowercapRaplProfiler
from powermetrics import PowerMetricsProfiler

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


def is_powermetrics_available():
    if (
        (platform.system() == "Darwin")
        and (platform.machine() == "arm64")
        and (os.geteuid() == 0)
        and (which("powermetrics") is not None)
    ):
        return True
    else:
        return False


def is_powercap_available():
    if (
        (platform.processor() == "x86_64")
        and platform.system() == "Linux"
        and os.geteuid() == 0
        and os.path.exists("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0")
    ):
        return True
    else:
        return False


def collect_stats(db):
    return {
        "total_rows_RHS": db.con.execute("select count(*) from perf").fetchall()[0][0],
        "row_count_LHS": db.con.execute("select count(*) from acq").fetchall()[0][0],
    }


def aggregate_power_stats(power_results):
    cpus = pd.json_normalize(power_results, ["processor", "clusters", "cpus"])
    clusters = pd.json_normalize(power_results, ["processor", "clusters"])
    total = pd.json_normalize(power_results)
    return {
        #"idle_ratio_cpus": list(cpus.groupby("cpu").idle_ratio.mean()),
        #"freq_hz": list(cpus.groupby("cpu").freq_hz.mean()),
        "power_mW": sum(list(clusters.groupby("name").mean().power.values)),
        #"package_energy_sum": int(total["processor.package_energy"].sum()),
        "cpu_mJ": int(total["processor.cpu_energy"].sum()),
        #"dram_energy_sum": int(total["processor.dram_energy"].sum()),
        #"elapsed_ns": int(total["elapsed_ns"].sum()),
    }


QUERIES = {"summary": summary_query}


def execute(expr, db):
    result = db.execute(expr)
    return result


def profile_run(expression, db):
    start_time_process = timeit.default_timer()
    start_time_cpu = time.process_time()
    mem = memory_usage((execute, (expression, db,),))
    total_time_cpu = time.process_time() - start_time_cpu
    total_time_process = timeit.default_timer() - start_time_process
    return mem, total_time_process, total_time_cpu


def run_query(query, db, powermetrics):
    expression = QUERIES[query](db)
    if powermetrics and is_powermetrics_available():
        with PowerMetricsProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = aggregate_power_stats(power.results)
    elif powermetrics and is_powercap_available():
        with PowercapRaplProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {"cpu_mJ": power.results/10 ** 3, "power_mW": power.results/power.total_time/10**3}
    else:
        mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {}

    run_stats = {
        "name": query,
        "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_time_process": total_time_process,
        "total_time_cpu": total_time_cpu,
        "max_memory_usage": max(mem),
        "incremental_memory_usage": mem[-1] - mem[0],
    }
    run_stats.update(power_cpu)
    return run_stats


@click.command()
@click.option(
    "--powermetrics/--no-powermetrics",
    default=False,
    show_default=True,
    help="Flag to get cpu and power metrics on OSX",
)
@click.option(
    "--threads",
    default=str(psutil.cpu_count()),
    show_default=True,
    help="comma seperated list of threads to run e.g. 2,4,8",
)
@click.option("--datadir", default="data", show_default=True)
def main(datadir, threads, powermetrics):
    threads = [int(s) for s in threads.split(",")]
    click.echo("[")
    for i, t in enumerate(threads):
        datadir = Path(datadir)
        db = create_duckdb(datadir, threads=t, profile="profile.txt")
        stats = [run_query(query, db, powermetrics) for query in QUERIES]

        data = {
            **platform_info(),
            "runs": stats,
            "threads": t,
            "schema_info": collect_stats(db),
            "datadir": str(datadir),
        }
        click.echo(json.dumps(data))
        total_threads = len(threads)
        if total_threads > 1 and i < total_threads - 1:
            click.echo(",")

    click.echo("]")


if __name__ == "__main__":
    main()
