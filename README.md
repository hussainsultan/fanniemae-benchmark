## [WIP] Single-Family Loan Performance
Dataset is derived from Fannie Mae’s Single-Family Loan Performance Data with all rights reserved by Fannie Mae and made available [here](https://docs.rapids.ai/datasets/mortgage-data) by RAPIDS team. 

### Prepare Data
The following script will download the data and parse it into parquet files

```
mkdir data && python prepare.py --years=1 --datadir=data
```
### Run Summary Benchmark

```
python run.py --mode=sql --datadir=data --threads=8
```

