import duckdb
import pyarrow.parquet as pq
import pyarrow.compute as pac
import pyarrow as pa

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch")
con.execute("CALL dbgen(sf=100)")

tables =["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

for t in tables:
    parquet_table = con.query(f"SELECT * FROM {t}").arrow()
    for i, (col_name, type_) in enumerate(zip(parquet_table.schema.names, parquet_table.schema.types)):
      if pa.types.is_decimal(type_):
        parquet_table = parquet_table.set_column(i, col_name, pac.cast(parquet_table.column(col_name), pa.float64()))
        pq.write_table(parquet_table,f"sf100/{t}.parquet")
      else:
          pq.write_table(parquet_table, f"sf100/{t}.parquet")


