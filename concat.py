import click
import duckdb


@click.command()
@click.option("--datadir", default="data")
def concat(datadir):
    import duckdb
    conn = duckdb.connect()
    conn.execute(f"""COPY (select * from '{datadir}/perf/*.parquet') TO 
                    '{datadir}/perf/perf.parquet'( FORMAT 'parquet')""") 
    conn.execute(f"""COPY (select * from '{datadir}/acq/*.parquet') TO 
                    '{datadir}/perf/acq.parquet'( FORMAT 'parquet')""") 


if __name__=="__main__":
    concat()
