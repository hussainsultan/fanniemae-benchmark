## [WIP] Single-Family Loan Performance
Dataset is derived from Fannie Mae’s Single-Family Loan Performance Data with all rights reserved by Fannie Mae and made available [here](https://docs.rapids.ai/datasets/mortgage-data) by RAPIDS team. 
## Example Results
| name run\_date total\_time\_process total\_time\_cpu max\_memory\_usage incremental\_memory\_usage power\_mW cpu\_mJ dram\_energy_sum datadir db |
| --- |
| 0 summary 20/02/2023 22:15:42 1.802674 4.494954 334.015625 164.140625 13175.0 17478 384 oneyear duckdb |
| 1 summary 20/02/2023 22:15:48 4.924769 11.276125 4317.171875 384.000000 5919.0 26257 2784 oneyear polars |
| 2 summary 20/02/2023 22:15:53 3.991306 17.979814 1205.187500 481.000000 15247.0 53902 1191 twoyear duckdb |
| 3 summary 20/02/2023 22:16:20 26.730788 59.804207 7612.265625 -454.562500 5451.0 143087 17936 twoyear polars |
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

