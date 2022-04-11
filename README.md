## [WIP] Single-Family Loan Performance
Dataset is derived from Fannie Mae’s Single-Family Loan Performance Data with all rights reserved by Fannie Mae and made available [here](https://docs.rapids.ai/datasets/mortgage-data) by RAPIDS team. 

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

