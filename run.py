import datetime
import itertools
import json
import multiprocessing
import os
import platform
import sys
import time
import timeit
import traceback
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

from benchmark.fanniemae_summary import summary_query
from benchmark.powercap_rapl import PowercapRaplProfiler
from benchmark.powermetrics import PowerMetricsProfiler

warnings.filterwarnings("ignore")
# Fix
sys.path.append("tpc-queries")

from ibis_tpc import (h01, h02, h03, h04, h05, h06, h07, h08, h09, h10, h11,
                      h12, h13, h14, h15, h16, h17, h18, h19, h20, h21, h22)

BACKENDS = {"polars": ibis.polars.connect(), "duckdb": ibis.duckdb.connect()}

QUERIES_TPCH = {
    "h01": h01.tpc_h01,
    "h02": h02.tpc_h02,
    "h03": h03.tpc_h03,
    "h04": h04.tpc_h04,
    "h05": h05.tpc_h05,
    "h06": h06.tpc_h06,
    "h07": h07.tpc_h07,
    "h08": h08.tpc_h08,
    "h09": h09.tpc_h09,
    "h10": h10.tpc_h10,
    "h11": h11.tpc_h11,
    "h12": h12.tpc_h12,
    "h13": h13.tpc_h13,
    "h14": h14.tpc_h14,
    "h15": h15.tpc_h15,
    "h16": h16.tpc_h16,
    "h17": h17.tpc_h17,
    "h18": h18.tpc_h18,
    "h19": h19.tpc_h19,
    "h20": h20.tpc_h20,
    "h21": h21.tpc_h21,
    "h22": h22.tpc_h22,
}


def setup_tpch_db(datadir, engine="duckdb", threads=8):
    db = BACKENDS.get(engine)
    tables = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    for t in tables:
        path = datadir / f"{t}.parquet"
        db.register(f"{path}", t)
    db.con.execute(f"PRAGMA threads={threads};")
    return db


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


def aggregate_power_stats(power_results):
    cpus = pd.json_normalize(power_results, ["processor", "clusters", "cpus"])
    clusters = pd.json_normalize(power_results, ["processor", "clusters"])
    total = pd.json_normalize(power_results)
    return {
        "idle_ratio_cpus": list(cpus.groupby("cpu").idle_ratio.mean()),
        "freq_hz": list(cpus.groupby("cpu").freq_hz.mean()),
        "power_mW": sum(list(clusters.groupby("name").mean().power.values)),
        "package_energy_sum": int(total["processor.package_energy"].sum()),
        "cpu_mJ": int(total["processor.cpu_energy"].sum()),
        "dram_energy_sum": int(total["processor.dram_energy"].sum()),
        "elapsed_ns": int(total["elapsed_ns"].sum()),
    }


def execute(expr):
    result = expr.execute()
    return result


def profile_run(expression, db):
    start_time_process = timeit.default_timer()
    start_time_cpu = time.process_time()
    mem = memory_usage(
        (
            execute,
            (expression,),
        )
    )
    total_time_cpu = time.process_time() - start_time_cpu
    total_time_process = timeit.default_timer() - start_time_process
    return mem, total_time_process, total_time_cpu


def run_query(query, powermetrics, datadir, engine, threads=8):
    db = setup_tpch_db(datadir, engine, threads)
    expression = QUERIES_TPCH[query](db)
    if powermetrics and is_powermetrics_available():
        with PowerMetricsProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = aggregate_power_stats(power.results)
    elif powermetrics and is_powercap_available():
        with PowercapRaplProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {
            "cpu_mJ": power.results / 10**3,
            "power_mW": power.results / power.total_time / 10**3,
        }
    else:
        mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {}

    run_stats = {
        "name": query,
        "threads": threads,
        "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_time_process": total_time_process,
        "total_time_cpu": total_time_cpu,
        "max_memory_usage": max(mem),
    }
    run_stats.update(power_cpu)
    return run_stats


def run_query_fannie(powermetrics, datadir, engine, threads=8):
    db = BACKENDS.get(engine)
    perf_path = datadir / "perf/*.parquet"
    acq_path = datadir / "acq/*.parquet"
    db.register(f"{perf_path}", "perf")
    db.register(f"{acq_path}", "acq")
    db.con.execute(f"PRAGMA threads={threads};")
    expression = summary_query(db)
    if powermetrics and is_powermetrics_available():
        with PowerMetricsProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = aggregate_power_stats(power.results)
    elif powermetrics and is_powercap_available():
        with PowercapRaplProfiler() as power:
            mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {
            "cpu_mJ": power.results / 10**3,
            "power_mW": power.results / power.total_time / 10**3,
        }
    else:
        mem, total_time_process, total_time_cpu = profile_run(expression, db)
        power_cpu = {}

    run_stats = {
        "name": "Summary",
        "threads": threads,
        "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_time_process": total_time_process,
        "total_time_cpu": total_time_cpu,
        "max_memory_usage": max(mem),
    }
    run_stats.update(power_cpu)
    return run_stats


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "--threads",
    default="8",
    show_default=True,
    help="comma seperated list of threads to run to run",
)
@click.option(
    "--queries",
    default="h01,h02,h03,h04,h05,h06,h07,h08,h09,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22",
    show_default=True,
    help="comma seperated list of questions to run",
)
@click.option(
    "--engines",
    default="duckdb",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. duckdb, polars",
)
@click.option(
    "--powermetrics/--no-powermetrics",
    default=False,
    show_default=True,
    help="Flag to get cpu and power metrics on OSX",
)
@click.option(
    "--datadir",
    default="data",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. 2,4,8",
)
def tpch(datadir, powermetrics, engines, queries, threads):
    datadirs = [s for s in datadir.split(",")]
    engines = [s for s in engines.split(",")]
    queries = [s for s in queries.split(",")]
    threads = [int(s) for s in threads.split(",")]
    runs = []
    for datadir, engine, thread in itertools.product(datadirs, engines, threads):
        datadir = Path(datadir)
        stats = [
            run_query(query, powermetrics, datadir, engine, thread) for query in queries
        ]

        data = {**platform_info(), "runs": stats, "datadir": datadir, "db": engine}
        runs.append(data)

    df = pd.json_normalize(runs, ["runs"], meta=["datadir", "db"])
    click.echo(df.to_csv(index=False))


@click.command()
@click.option(
    "--threads",
    default="8",
    show_default=True,
    help="comma seperated list of threads to run",
)
@click.option(
    "--engines",
    default="duckdb",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. duckdb, polars",
)
@click.option(
    "--powermetrics/--no-powermetrics",
    default=False,
    show_default=True,
    help="Flag to get cpu and power metrics on OSX",
)
@click.option(
    "--datadir",
    default="data",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. 2,4,8",
)
def fanniemae(datadir, powermetrics, engines, threads):
    datadirs = [s for s in datadir.split(",")]
    engines = [s for s in engines.split(",")]
    threads = [s for s in threads.split(",")]
    runs = []
    for datadir, engine, thread in itertools.product(datadirs, engines, threads):
        datadir = Path(datadir)
        stats = [run_query_fannie(powermetrics, datadir, engine, thread)]
        data = {**platform_info(), "runs": stats, "datadir": datadir, "db": engine}
        runs.append(data)

    df = pd.json_normalize(runs, ["runs"], meta=["datadir", "db"])
    click.echo(df.to_csv(index=False))


cli.add_command(tpch)
cli.add_command(fanniemae)

if __name__ == "__main__":
    cli()
