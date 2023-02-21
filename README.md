## [WIP] Single-Family Loan Performance
Dataset is derived from Fannie Mae’s Single-Family Loan Performance Data with all rights reserved by Fannie Mae and made available [here](https://docs.rapids.ai/datasets/mortgage-data) by RAPIDS team. 
## Example Results
|id|name                         |run_date|total_time_process                           |total_time_cpu    |max_memory_usage|incremental_memory_usage|power_mW|cpu_mJ|dram_energy_sum|datadir|db    |
|------|-----------------------------|--------|---------------------------------------------|------------------|----------------|------------------------|--------|------|---------------|-------|------|
|0     |summary                      |20/02/2023 22:17:15|1.7020075409673154                           |3.375095          |333.140625      |164.25                  |12412.0 |15240 |337            |oneyear|duckdb|
|1     |summary                      |20/02/2023 22:17:20|3.9095819171052426                           |10.747104         |5862.703125     |719.53125               |6795.0  |23351 |2186           |oneyear|polars|
|2     |summary                      |20/02/2023 22:17:25|3.9357682089321315                           |18.247276999999997|1408.171875     |352.703125              |15384.0 |52811 |1078           |twoyear|duckdb|
|3     |summary                      |20/02/2023 22:17:51|26.165474832989275                           |56.67650499999999 |7933.53125      |-662.390625             |5309.0  |136470|16505          |twoyear|polars|

### Prepare Data
The following script will download the data and parse it into parquet files

```
❯ python prepare.py --help
Usage: prepare.py [OPTIONS]

Options:
  --with-id-as-float64 / --without-id-as-float64
                                  [default: without-id-as-float64]
  --years [1|2|4|8|16|17]         Number of years of fannie mae data to
                                  download  [default: 1]
  --datadir TEXT                  directory to download the data
  --help                          Show this message and exit.
```
### Run

```
❯ python run.py --help
Usage: run.py [OPTIONS]
Options:
  --powermetrics / --no-powermetrics
                                  Flag to get cpu and power metrics on OSX
                                  [default: no-powermetrics]
  --threads TEXT                  comma seperated list of threads to run e.g.
                                  2,4,8  [default: 8]
  --datadir TEXT                  [default: data]
  --help                          Show this message and exit.
```

