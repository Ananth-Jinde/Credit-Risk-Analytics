# Data Dictionary

## Bronze Layer

### `credit_risk_analytics.bronze.raw_loan_data`

| Column | Type | Description |
|--------|------|-------------|
| id | STRING | Original loan identifier |
| member_id | STRING | Generated surrogate key (SHA-256 hash) |
| loan_amnt | DOUBLE | Requested loan amount ($) |
| funded_amnt | DOUBLE | Amount funded by investors ($) |
| term | STRING | Loan term (e.g., " 36 months") |
| int_rate | DOUBLE | Interest rate (%) |
| installment | DOUBLE | Monthly installment ($) |
| grade | STRING | Borrower grade (A-G) |
| sub_grade | STRING | Borrower sub-grade (A1-G5) |
| emp_title | STRING | Employment title (freeform) |
| emp_length | STRING | Employment length (e.g., "10+ years") |
| home_ownership | STRING | RENT, OWN, MORTGAGE, OTHER |
| annual_inc | DOUBLE | Annual income ($) |
| verification_status | STRING | Income verification status |
| issue_d | STRING | Loan issue date (e.g., "Dec-2015") |
| loan_status | STRING | Current loan status |
| purpose | STRING | Loan purpose category |
| addr_state | STRING | Borrower state (2-letter code) |
| dti | DOUBLE | Debt-to-income ratio |
| delinq_2yrs | DOUBLE | Delinquencies in past 2 years |
| pub_rec | DOUBLE | Number of public records |
| pub_rec_bankruptcies | DOUBLE | Number of bankruptcy records |
| inq_last_6mths | DOUBLE | Credit inquiries in last 6 months |
| total_pymnt | DOUBLE | Total payments received ($) |
| total_rec_prncp | DOUBLE | Total principal received ($) |
| total_rec_int | DOUBLE | Total interest received ($) |
| total_rec_late_fee | DOUBLE | Total late fees received ($) |
| last_pymnt_amnt | DOUBLE | Last payment amount ($) |
| recoveries | DOUBLE | Post-charge-off recoveries ($) |
| tot_hi_cred_lim | DOUBLE | Total high credit limit ($) |
| _ingested_at | TIMESTAMP | Pipeline ingestion timestamp |
| _source_file | STRING | Source file identifier |
| _batch_id | STRING | Batch run identifier |

---

## Silver Layer

### `credit_risk_analytics.silver.dim_customers`

| Column | Type | Description |
|--------|------|-------------|
| member_id | STRING | Surrogate customer key (PK) |
| emp_title | STRING | Original employment title |
| emp_category | STRING | Standardized job category (12 categories) |
| emp_length | INTEGER | Employment length in years |
| home_ownership | STRING | RENT, OWN, MORTGAGE |
| annual_income | DOUBLE | Annual income ($), outliers capped |
| income_bracket | STRING | Low, Lower-Middle, Middle, Upper-Middle, High |
| income_outlier_flag | BOOLEAN | True if original income exceeded cap |
| address_state | STRING | Validated 2-letter state code |
| address_region | STRING | Census region (Northeast, South, Midwest, West) |
| address_zipcode | STRING | Partial zip code |
| grade | STRING | Borrower grade (A-G) |
| sub_grade | STRING | Borrower sub-grade (A1-G5) |
| credit_utilization_ratio | DOUBLE | Income / credit limit ratio |
| total_high_credit_limit | DOUBLE | Total high credit limit ($) |
| application_type | STRING | Individual or Joint App |
| verification_status | STRING | Income verification status |
| effective_date | DATE | SCD2: record effective date |
| end_date | DATE | SCD2: record end date (9999-12-31 if current) |
| is_current | BOOLEAN | SCD2: true for current version |

### `credit_risk_analytics.silver.fact_loans`

| Column | Type | Description |
|--------|------|-------------|
| loan_id | STRING | Loan identifier (PK) |
| member_id | STRING | Customer foreign key |
| loan_amount | DOUBLE | Requested loan amount ($) |
| funded_amount | DOUBLE | Amount funded ($) |
| funded_amount_gap | DOUBLE | Loan amount - funded amount |
| funded_pct | DOUBLE | Funded / requested ratio |
| interest_rate | DOUBLE | Interest rate (%) |
| interest_rate_band | STRING | Low, Medium, High, Very High |
| monthly_installment | DOUBLE | Monthly installment ($) |
| loan_term_months | INTEGER | Loan term in months (36 or 60) |
| issue_date | DATE | Loan issue date |
| issue_year | INTEGER | Issue year |
| issue_month | INTEGER | Issue month |
| vintage_month | STRING | YYYY-MM for cohort analysis |
| loan_status | STRING | Original loan status |
| loan_status_category | STRING | FULLY_PAID, CURRENT, LATE, CHARGED_OFF, etc. |
| loan_purpose | STRING | Loan purpose category |
| loan_to_income_ratio | DOUBLE | Loan amount / annual income |

### `credit_risk_analytics.silver.fact_loan_repayments`

| Column | Type | Description |
|--------|------|-------------|
| loan_id | STRING | Loan identifier (PK) |
| member_id | STRING | Customer foreign key |
| total_principal_received | DOUBLE | Total principal collected ($) |
| total_interest_received | DOUBLE | Total interest collected ($) |
| total_late_fee_received | DOUBLE | Total late fees collected ($) |
| total_payment_received | DOUBLE | Total payments received ($) |
| last_payment_amount | DOUBLE | Most recent payment ($) |
| last_payment_date | DATE | Date of last payment |
| next_payment_date | DATE | Expected next payment date |
| monthly_installment | DOUBLE | Expected monthly installment |
| outstanding_principal | DOUBLE | Remaining principal balance |
| recoveries | DOUBLE | Post-charge-off recoveries ($) |
| payment_completion_ratio | DOUBLE | Total payments / loan amount |
| late_payment_flag | BOOLEAN | True if any late fees charged |
| recovery_rate | DOUBLE | Recoveries / outstanding principal |
| months_on_books | INTEGER | Months since issue to last payment |
| payment_to_installment_ratio | DOUBLE | Last payment / installment |

### `credit_risk_analytics.silver.dim_loan_defaulters`

| Column | Type | Description |
|--------|------|-------------|
| member_id | STRING | Customer identifier (PK) |
| delinq_2yrs | INTEGER | Delinquencies in past 2 years |
| delinq_amount | DOUBLE | Total delinquent amount ($) |
| months_since_last_delinq | INTEGER | Months since last delinquency |
| public_records | INTEGER | Number of public records |
| public_bankruptcies | INTEGER | Number of bankruptcies |
| inquiries_last_6months | INTEGER | Credit inquiries (last 6 months) |
| accounts_now_delinquent | INTEGER | Currently delinquent accounts |
| collections_12months | INTEGER | Collections in past 12 months |
| chargeoffs_12months | INTEGER | Charge-offs in past 12 months |
| tax_liens | INTEGER | Number of tax liens |
| delinquency_aging | STRING | NEVER_DELINQUENT, RECENT, MODERATE, AGED, VERY_OLD |
| default_risk_tier | STRING | VERY_HIGH, HIGH, MODERATE, LOW |
| compound_risk_indicator | DOUBLE | Weighted risk score (0-100+) |

---

## Gold Layer

### `credit_risk_analytics.gold.credit_risk_scores`

| Column | Type | Description |
|--------|------|-------------|
| member_id | STRING | Customer identifier |
| loan_id | STRING | Loan identifier |
| grade | STRING | Borrower grade |
| annual_income | DOUBLE | Annual income ($) |
| income_bracket | STRING | Income bracket category |
| address_state | STRING | Borrower state |
| address_region | STRING | Census region |
| payment_history_score | DOUBLE | Weighted payment score (20%) |
| default_history_score | DOUBLE | Weighted default score (45%) |
| financial_health_score | DOUBLE | Weighted financial score (35%) |
| credit_risk_score | DOUBLE | Composite risk score |
| credit_risk_grade | STRING | Final grade: A, B, C, D, E, F |
