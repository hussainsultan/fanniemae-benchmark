import glob
import tarfile

import click
import dask.bag as db
import pyarrow
import pyarrow.csv as pc
import pyarrow.parquet as pq
import wget

from pathlib import Path

LINKS = {
    1: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000.tgz",
    2: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2001.tgz",
    4: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2003.tgz",
    8: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2007.tgz",
    16: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2015.tgz",
    17: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2016.tgz"
}


@click.command()
@click.option("--years", default=1)
@click.option("--datadir", default="data")
def main(years, datadir):
    link = LINKS[years]
    print("Downloading ...")
    wget.download(link, datadir)
    print("Extracting ...")
    tar = tarfile.open(Path(datadir) / link.split("/")[-1])
    tar.extractall(datadir)
    tar.close()
    print("Converting to parquet ...")
    extracted_files = (Path(datadir) / "perf").glob("*.txt*")
    db.from_sequence(extracted_files).map(convert_csv).compute()


def convert_csv(f):
    columns = {
        "loan_id": pyarrow.int64(),
        "monthly_reporting_period": pyarrow.string(),
        "servicer": pyarrow.string(),
        "interest_rate": pyarrow.decimal128(2),
        "current_actual_upb": pyarrow.decimal128(2),
        "loan_age": pyarrow.int16(),
        "remaining_months_to_legal_maturity": pyarrow.int64(),
        "adj_remaining_months_to_maturity": pyarrow.decimal128(2),
        "maturity_date": pyarrow.string(),
        "msa": pyarrow.decimal128(2),
        "current_loan_delinquency_status": pyarrow.int8(),
        "mod_flag": pyarrow.string(),
        "zero_balance_code": pyarrow.string(),
        "zero_balance_effective_date": pyarrow.string(),
        "last_paid_installment_date": pyarrow.string(),
        "foreclosed_after": pyarrow.string(),
        "disposition_date": pyarrow.string(),
        "foreclosure_costs": pyarrow.int64(),
        "prop_preservation_and_reair_costs": pyarrow.decimal128(2),
        "asset_recovery_costs": pyarrow.decimal128(2),
        "misc_holding_expenses": pyarrow.decimal128(2),
        "holding_taxes": pyarrow.decimal128(2),
        "net_sale_proceeds": pyarrow.decimal128(2),
        "credit_enhancement_proceeds": pyarrow.decimal128(2),
        "repurchase_make_whole_proceeds": pyarrow.decimal128(2),
        "other_foreclosure_proceeds": pyarrow.decimal128(2),
        "non_interest_bearing_upb": pyarrow.decimal128(2),
        "principal_forgiveness_upb": pyarrow.string(),
        "repurchase_make_whole_proceeds_flag": pyarrow.string(),
        "foreclosure_principal_write_off_amount": pyarrow.string(),
        "servicing_activity_indicator": pyarrow.string(),
    }
    cvs_options = pc.ConvertOptions(column_types=columns)
    parse_options = pc.ParseOptions(delimiter="|")
    data = pc.read_csv(
        f,
        convert_options=cvs_options,
        parse_options=parse_options,
    )
    #outfile = f.split(".")[0] + ".parquet"
    outfile = f.rename(f.with_suffix('.parquet'))
    pq.write_table(data, outfile)


if __name__ == "__main__":
    main()
