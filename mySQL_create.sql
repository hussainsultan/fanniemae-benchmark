DROP DATABASE IF EXISTS mortgage_analysis;
CREATE DATABASE mortgage_analysis;
USE mortgage_analysis;
DROP TABLE IF EXISTS acq;
CREATE TABLE acq (id DOUBLE AUTO_INCREMENT, loan_id DOUBLE, orig_channel VARCHAR(255), seller_name VARCHAR(255), orig_interest_rate DOUBLE, orig_upb DOUBLE, orig_loan_term DOUBLE, orig_date VARCHAR(255), first_pay_date VARCHAR(255), orig_ltv DOUBLE, orig_cltv DOUBLE, num_borrowers DOUBLE, dti DOUBLE, borrower_credit_score DOUBLE, first_home_buyer VARCHAR(255), loan_purpose VARCHAR(255), property_type VARCHAR(255), num_units DOUBLE, occupancy_status VARCHAR(255), property_state VARCHAR(255), zip DOUBLE, mortgage_insurance_percent DOUBLE, product_type VARCHAR(255), coborrow_credit_score DOUBLE, mortgage_insurance_type DOUBLE, relocation_mortgage_indicator VARCHAR(255), dummy VARCHAR(255), PRIMARY KEY (id));
DROP TABLE IF EXISTS perf;
CREATE TABLE perf (id DOUBLE AUTO_INCREMENT, loan_id DOUBLE, monthly_reporting_period VARCHAR(255), servicer VARCHAR(255), interest_rate DECIMAL(6,3), current_actual_upb DECIMAL(12,2), loan_age DOUBLE, remaining_months_to_legal_maturity DOUBLE, adj_remaining_months_to_maturity DOUBLE, maturity_date VARCHAR(255), msa DOUBLE, current_loan_delinquency_status DOUBLE, mod_flag VARCHAR(255), zero_balance_code VARCHAR(255), zero_balance_effective_date VARCHAR(255), last_paid_installment_date VARCHAR(255), foreclosed_after VARCHAR(255), disposition_date VARCHAR(255), foreclosure_costs DOUBLE, prop_preservation_and_reair_costs DOUBLE, asset_recovery_costs DOUBLE, misc_holding_expenses DOUBLE, holding_taxes DOUBLE, net_sale_proceeds DOUBLE, credit_enhancement_proceeds DOUBLE, repurchase_make_whole_proceeds DOUBLE, other_foreclosure_proceeds DOUBLE, non_interest_bearing_upb DOUBLE, principal_forgiveness_upb VARCHAR(255), repurchase_make_whole_proceeds_flag VARCHAR(255), foreclosure_principal_write_off_amount VARCHAR(255), servicing_activity_indicator VARCHAR(255), PRIMARY KEY (id));