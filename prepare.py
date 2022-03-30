import glob
import tarfile
from pathlib import Path

import click
import dask.bag as db
import pyarrow
import pyarrow.csv as pc
import pyarrow.parquet as pq
import wget

LINKS = {
    1: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000.tgz",
    2: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2001.tgz",
    4: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2003.tgz",
    8: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2007.tgz",
    16: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2015.tgz",
    17: "http://rapidsai-data.s3-website.us-east-2.amazonaws.com/notebook-mortgage-data/mortgage_2000-2016.tgz",
}


@click.command()
@click.option("--with-float64", default=False)
@click.option("--years", default=1)
@click.option("--datadir", default="data")
def main(years, datadir,with_float64):
    link = LINKS[years]
    print("Downloading ...")
    wget.download(link, datadir)
    print("Extracting ...")
    tar = tarfile.open(Path(datadir) / link.split("/")[-1])
    tar.extractall(datadir)
    tar.close()
    print("Converting to parquet ...")
    extracted_files = (Path(datadir) / "perf").glob("*.txt*")
    db.from_sequence(extracted_files).map(convert_csv,(with_float64,)).compute()

    extracted_files = (Path(datadir) / "acq").glob("*.txt*")
    db.from_sequence(extracted_files).map(convert_acq).compute()


def convert_csv(f, with_float64=False):
    columns = {
        "loan_id": [pyarrow.float64() if with_float64 else pyarrow.int64()][0],
        "monthly_reporting_period": pyarrow.string(),
        "servicer": pyarrow.string(),
        "interest_rate": pyarrow.float64(),
        "current_actual_upb": pyarrow.float64(),
        "loan_age": pyarrow.float64(),
        "remaining_months_to_legal_maturity": pyarrow.float64(),
        "adj_remaining_months_to_maturity": pyarrow.float64(),
        "maturity_date": pyarrow.string(),
        "msa": pyarrow.float64(),
        "current_loan_delinquency_status": pyarrow.float64(),
        "mod_flag": pyarrow.string(),
        "zero_balance_code": pyarrow.string(),
        "zero_balance_effective_date": pyarrow.string(),
        "last_paid_installment_date": pyarrow.string(),
        "foreclosed_after": pyarrow.string(),
        "disposition_date": pyarrow.string(),
        "foreclosure_costs": pyarrow.float64(),
        "prop_preservation_and_reair_costs": pyarrow.float64(),
        "asset_recovery_costs": pyarrow.float64(),
        "misc_holding_expenses": pyarrow.float64(),
        "holding_taxes": pyarrow.float64(),
        "net_sale_proceeds": pyarrow.float64(),
        "credit_enhancement_proceeds": pyarrow.float64(),
        "repurchase_make_whole_proceeds": pyarrow.float64(),
        "other_foreclosure_proceeds": pyarrow.float64(),
        "non_interest_bearing_upb": pyarrow.float64(),
        "principal_forgiveness_upb": pyarrow.string(),
        "repurchase_make_whole_proceeds_flag": pyarrow.string(),
        "foreclosure_principal_write_off_amount": pyarrow.string(),
        "servicing_activity_indicator": pyarrow.string(),
    }
    cvs_options = pc.ConvertOptions(column_types=columns)
    parse_options = pc.ParseOptions(delimiter="|")
    read_options = pc.ReadOptions(column_names=columns.keys())
    data = pc.read_csv(
        f,
        convert_options=cvs_options,
        parse_options=parse_options,
        read_options=read_options,
    )
    outfile = f.parent / (f.name + ".parquet")
    pq.write_table(data, outfile)


def convert_acq(f):
    columns = {
        "loan_id": pyarrow.int64(),
        "orig_channel": pyarrow.string(),
        "seller_name": pyarrow.string(),
        "orig_interest_rate": pyarrow.float64(),
        "orig_upb": pyarrow.float64(),
        "orig_loan_term": pyarrow.float64(),
        "orig_date": pyarrow.string(),
        "first_pay_date": pyarrow.string(),
        "orig_ltv": pyarrow.float64(),
        "orig_cltv": pyarrow.float64(),
        "num_borrowers": pyarrow.float64(),
        "dti": pyarrow.float64(),
        "borrower_credit_score": pyarrow.float64(),
        "first_home_buyer": pyarrow.string(),
        "loan_purpose": pyarrow.string(),
        "property_type": pyarrow.string(),
        "num_units": pyarrow.float64(),
        "occupancy_status": pyarrow.string(),
        "property_state": pyarrow.string(),
        "zip": pyarrow.float64(),
        "mortgage_insurance_percent": pyarrow.float64(),
        "product_type": pyarrow.string(),
        "coborrow_credit_score": pyarrow.float64(),
        "mortgage_insurance_type": pyarrow.float64(),
        "relocation_mortgage_indicator": pyarrow.string(),
        "dummy": pyarrow.string(),
    }

    cvs_options = pc.ConvertOptions(column_types=columns)
    read_options = pc.ReadOptions(column_names=columns.keys())
    parse_options = pc.ParseOptions(delimiter="|")
    data = pc.read_csv(
        f,
        convert_options=cvs_options,
        parse_options=parse_options,
        read_options=read_options,
    )
    outfile = f.parent / (f.name + ".parquet")
    pq.write_table(data, outfile)


if __name__ == "__main__":
    main()
