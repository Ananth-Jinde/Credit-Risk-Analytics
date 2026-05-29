# Project Flow — Credit Risk Analytics

## End-to-End Pipeline Flow

This document shows exactly what happens when the pipeline runs, step by step.

---

## Stage 1: Bronze Ingestion

**Notebook**: `01_bronze_ingestion.py`
**Module**: `src/ingestion/bronze_loader.py`
**Input**: Raw CSV file (2.2M rows × 118 columns)
**Output**: `credit_risk_analytics.bronze.raw_loan_data` (Delta table)

```
CSV File
  │
  ├── 1. Read with explicit schema (118 columns defined upfront)
  │      No inferSchema — faster and safer
  │
  ├── 2. Filter garbage rows
  │      Remove ~33 rows where loan_amnt, grade, annual_inc are all NULL
  │
  ├── 3. Generate member_id
  │      SHA-256 hash of (emp_title, emp_length, home_ownership, income, state)
  │      Creates a deterministic surrogate key
  │
  ├── 4. Add audit columns
  │      _ingested_at: current timestamp
  │      _source_file: filename
  │      _batch_id: unique batch identifier
  │
  └── 5. Write to Delta table (bronze.raw_loan_data)
         Mode: overwrite (full refresh)
```

**Validation**: Inline checks verify the table is not empty and critical columns (`loan_amnt`, `grade`, `member_id`) have zero nulls.

---

## Stage 2: Silver Transformations

**Notebook**: `02_silver_transformations.py`
**Modules**: 4 transformer files in `src/transformations/`
**Input**: `credit_risk_analytics.bronze.raw_loan_data`
**Output**: 4 Silver Delta tables

### 2a. dim_customers (Customer Dimension)

```
Bronze raw_loan_data
  │
  ├── Select customer columns (emp_title, emp_length, home_ownership, income, state, grade, ...)
  │
  ├── Clean employment length
  │      "10+ years" → 10, "< 1 year" → 1, null → column mean
  │
  ├── Standardize job titles
  │      500K+ freeform titles → 12 categories (TECHNOLOGY, HEALTHCARE, etc.)
  │      Uses keyword matching: "software" in title → TECHNOLOGY
  │
  ├── Validate state codes
  │      Valid: "CA" → keep as-is, add region "West"
  │      Invalid: "XY" → replace with "NA"
  │
  ├── Cap income outliers
  │      Income > $10M → capped at $10M, income_outlier_flag = True
  │
  ├── Derive features
  │      income_bracket: Low / Lower-Middle / Middle / Upper-Middle / High
  │      credit_utilization_ratio: income / credit_limit
  │
  ├── Add SCD Type 2 columns
  │      effective_date, end_date (9999-12-31), is_current (True)
  │
  └── Deduplicate by member_id (window function, keep highest income)
```

### 2b. fact_loans (Loan Fact Table)

```
Bronze raw_loan_data
  │
  ├── Select loan columns (id, loan_amnt, funded_amnt, int_rate, term, issue_d, ...)
  │
  ├── Parse dates: "Dec-2015" → 2015-12-01
  │
  ├── Normalize term: " 36 months" → integer 36
  │
  ├── Classify interest rate band
  │      < 8% → Low, 8-14% → Medium, 14-20% → High, > 20% → Very High
  │
  ├── Standardize loan status
  │      Map raw statuses into: FULLY_PAID, CURRENT, LATE, CHARGED_OFF, DEFAULT, OTHER
  │
  ├── Derive features
  │      loan_to_income_ratio: loan_amount / annual_income
  │      funded_amount_gap: loan_amount - funded_amount
  │      vintage_month: "2015-12" (for grouping by origination month)
  │
  └── Deduplicate by loan_id
```

### 2c. fact_loan_repayments (Repayment Fact Table)

```
Bronze raw_loan_data
  │
  ├── Select repayment columns (total_pymnt, total_rec_prncp, total_rec_int, ...)
  │
  ├── payment_completion_ratio: total_payments / loan_amount
  │      1.0 = fully repaid, 0.5 = half repaid
  │
  ├── late_payment_flag: True if total_rec_late_fee > 0
  │
  ├── recovery_rate: recoveries / outstanding_principal
  │      How much was recovered after charge-off?
  │
  ├── payment_to_installment_ratio: last_payment / monthly_installment
  │
  └── months_on_books: months between issue_date and last_payment_date
```

### 2d. dim_loan_defaulters (Defaulter Dimension)

```
Bronze raw_loan_data
  │
  ├── Select default columns (delinq_2yrs, pub_rec, pub_rec_bankruptcies, ...)
  │
  ├── delinquency_aging bucket
  │      NEVER_DELINQUENT → RECENT → MODERATE → AGED → VERY_OLD
  │      Based on months_since_last_delinq
  │
  ├── default_risk_tier
  │      VERY_HIGH: delinq >= 5 or bankruptcies >= 2
  │      HIGH: delinq >= 3 or bankruptcies >= 1
  │      MODERATE: delinq >= 1
  │      LOW: no delinquencies
  │
  └── compound_risk_indicator (0-100+ weighted score)
        Combines: delinquencies × 10 + public_records × 20
        + bankruptcies × 30 + inquiries × 5 + collections × 15
```

**Validation**: Inline checks verify each table is not empty, primary keys have zero nulls, and duplicate counts are logged.

---

## Stage 3: Gold Analytics

**Notebook**: `03_gold_analytics.py`
**Module**: `src/analytics/credit_risk_scorer.py`
**Input**: All 4 Silver tables
**Output**: `credit_risk_analytics.gold.credit_risk_scores`

```
Silver tables (joined by member_id and loan_id)
  │
  ├── Join: dim_customers + fact_loans + fact_loan_repayments + dim_loan_defaulters
  │
  ├── Score Component 1: Payment History (20% weight)
  │      - Last payment vs installment amount
  │      - Total payments vs funded amount
  │      Sub-score: 0-800 points
  │
  ├── Score Component 2: Default History (45% weight)
  │      - Delinquencies in past 2 years
  │      - Public records, bankruptcies
  │      - Credit inquiries in last 6 months
  │      Sub-score: 0-800 points
  │
  ├── Score Component 3: Financial Health (35% weight)
  │      - Loan status (fully paid = good, charged off = bad)
  │      - Home ownership (own > mortgage > rent)
  │      - Credit utilization ratio
  │      - Lender-assigned grade (A-G)
  │      Sub-score: 0-800 points
  │
  ├── Composite Score = (Component1 × 0.20) + (Component2 × 0.45) + (Component3 × 0.35)
  │
  └── Letter Grade
        > 2500 → A (excellent)
        > 2000 → B (very good)
        > 1500 → C (good)
        > 1000 → D (fair)
        > 750  → E (poor)
        ≤ 750  → F (very poor)
```

---

## Pipeline Orchestration

```
+-------------------+       +-------------------+       +-------------------+
|  Task 1: Bronze   | ----> |  Task 2: Silver   | ----> |  Task 3: Gold     |
|  01_bronze_ingest |       |  02_silver_trans   |       |  03_gold_analytics|
|  ~3-5 min         |       |  ~5-8 min          |       |  ~3-5 min         |
+-------------------+       +-------------------+       +-------------------+
                                                          Total: ~15-20 min
```

Orchestrated via a **Databricks Lakeflow Job**, defined as-code in `resources/credit_risk_job.yml` and deployed via **Databricks Asset Bundles** (`databricks bundle deploy`).

- **Schedule**: Daily at 2:00 AM IST
- **Retry**: 2 retries per task on failure
- **Compute**: Jobs cluster (cheaper than interactive)
- **Alerts**: Email notification on failure

---

## Data Lineage

```
accepted_loans.csv
    └── bronze.raw_loan_data
            ├── silver.dim_customers
            ├── silver.fact_loans
            ├── silver.fact_loan_repayments
            ├── silver.dim_loan_defaulters
            │
            └── gold.credit_risk_scores
                    (joins all 4 Silver tables)
```
