# Feature Definitions and Transformations
## EU Budget Anomaly Detection Pipeline

**Version:** 1.0  
**Last Updated:** 2026-03-21  
**Total Features:** 22

---

## Feature Categories

| Category | Count | Purpose |
|----------|-------|---------|
| **Raw Features** | 8 | Direct from data sources |
| **H1 Features** | 5 | Net position analysis (contributor vs receiver) |
| **H3 Features** | 2 | Absorption anomaly detection |
| **Time Series** | 5 | Temporal patterns and trends |
| **Derived** | 2 | Statistical baselines (Z-scores) |

---

## 1. Raw Features (8)

### 1.1 `year`
- **Type:** Integer
- **Range:** 2000-2024
- **Source:** Excel sheet names
- **Nullable:** No
- **Description:** Fiscal year of the budget record
- **Transformation:** None (direct extraction)

### 1.2 `country`
- **Type:** String (ISO 3166-1 alpha-2)
- **Values:** 28 EU member states (AT, BE, BG, CY, CZ, DE, DK, EE, EL, ES, FI, FR, HR, HU, IE, IT, LT, LU, LV, MT, NL, PL, PT, RO, SE, SI, SK, UK)
- **Source:** Excel column headers
- **Nullable:** No
- **Description:** Country ISO code
- **Transformation:** None

### 1.3 `heading`
- **Type:** String (Categorical)
- **Values:** 8 unified categories
- **Source:** Row labels (with MFF crosswalk mapping)
- **Nullable:** Yes (if unmapped)
- **Description:** EU budget heading (category)
- **Transformation:** MFF_CROSSWALK dictionary mapping

**Heading Values:**
1. Agriculture & Rural Dev
2. Structural & Cohesion
3. Internal / Single Market
4. External & Global
5. Administration
6. Security & Citizenship
7. Pre-Accession
8. NGEU Recovery

### 1.4 `actual_meur`
- **Type:** Double (Decimal)
- **Unit:** Million Euros (€M)
- **Range:** 0.0 - 50,000.0
- **Source:** Excel data cells
- **Nullable:** Yes
- **Description:** Actual expenditure amount
- **Transformation:** Rounded to 4 decimal places

### 1.5 `earmarked_meur`
- **Type:** Double (Decimal)
- **Unit:** Million Euros (€M)
- **Range:** 0.0 - 60,000.0
- **Source:** Excel "Earmarked" column
- **Nullable:** Yes
- **Description:** Budgeted/allocated amount
- **Transformation:** Rounded to 4 decimal places

### 1.6 `is_ngeu`
- **Type:** Integer (Boolean)
- **Values:** 0 or 1
- **Source:** Row position (after "NextGenerationEU" marker)
- **Nullable:** No
- **Description:** Flag for Next Generation EU funding (COVID-19 recovery)
- **Transformation:** Set to 1 if row appears after NGEU marker, else 0
- **Purpose:** Exclude NGEU from historical analysis (temporary program)

### 1.7 `is_preliminary`
- **Type:** Integer (Boolean)
- **Values:** 0 or 1
- **Source:** Year comparison
- **Nullable:** No
- **Description:** Flag for preliminary/estimated data
- **Transformation:** Set to 1 if year == 2024, else 0
- **Purpose:** Exclude incomplete data from training

### 1.8 `heading_raw`
- **Type:** String
- **Source:** Original Excel row label
- **Nullable:** Yes
- **Description:** Original heading text before mapping
- **Transformation:** None (stored for audit trail)
- **Purpose:** Debugging and mapping validation

---

## 2. H1 Features - Net Position Analysis (5)

### 2.1 `receipts_meur`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Source:** Aggregated from expenditure data
- **Nullable:** Yes
- **Formula:**
```sql
SUM(actual_meur) WHERE is_ngeu = 0 GROUP BY year, country
```
- **Description:** Total receipts from EU budget per country-year (NGEU excluded)
- **Purpose:** Calculate net position (receipts - contributions)

### 2.2 `total_contributions`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Source:** Revenue data (pivoted)
- **Nullable:** Yes
- **Description:** Total national contributions to EU budget
- **Purpose:** Calculate net position

### 2.3 `gni_size`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Source:** Revenue data (Gross National Income)
- **Nullable:** Yes
- **Description:** Country's GNI (economic size indicator)
- **Purpose:** Normalize net position (net as % of GNI)

### 2.4 `net_position_meur`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Range:** -20,000 to +20,000
- **Formula:**
```sql
ROUND(receipts_meur - total_contributions, 2)
```
- **Description:** Net position (positive = net contributor, negative = net receiver)
- **Interpretation:**
  - **Positive:** Country contributes more than it receives (net contributor)
  - **Negative:** Country receives more than it contributes (net receiver)
- **Purpose:** H1 hypothesis testing (contributor/receiver drift)

### 2.5 `net_pct_gni`
- **Type:** Double
- **Unit:** Percentage
- **Range:** -5.0% to +2.0%
- **Formula:**
```sql
ROUND((receipts_meur - total_contributions) / NULLIF(gni_size, 0) * 100, 4)
```
- **Description:** Net position as percentage of GNI
- **Purpose:** Normalize across countries of different economic sizes
- **Interpretation:** Comparable across large (DE) and small (MT) countries

---

## 3. H3 Features - Absorption Analysis (2)

### 3.1 `absorption_gap_meur`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Range:** -10,000 to +10,000
- **Formula:**
```sql
ROUND(earmarked_meur - actual_meur, 2)
```
- **Description:** Under-spending (positive) or over-spending (negative)
- **Interpretation:**
  - **Positive:** Funds allocated but not spent (underabsorption)
  - **Negative:** Spent more than allocated (rare, usually corrections)
  - **Zero:** Perfect absorption
- **Purpose:** Detect absorption anomalies

### 3.2 `absorption_rate_pct`
- **Type:** Double
- **Unit:** Percentage
- **Range:** 0.0% to 100.0%+ (can exceed 100% for corrections)
- **Formula:**
```sql
ROUND(100.0 * actual_meur / NULLIF(earmarked_meur, 0), 2)
```
- **Description:** Percentage of allocated funds actually spent
- **Interpretation:**
  - **100%:** Perfect absorption
  - **< 85%:** Underabsorption (potential issue)
  - **> 100%:** Correction or carry-over from previous year
- **Purpose:** H3 hypothesis testing (identify chronic underabsorbers)

---

## 4. Time Series Features (5)

### 4.1 `yoy_change`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Formula:**
```sql
ROUND(actual_meur - LAG(actual_meur) OVER (PARTITION BY country, heading ORDER BY year), 2)
```
- **Description:** Year-over-year change in expenditure
- **Window:** Per country-heading partition
- **Nullable:** Yes (NULL for first year of each partition)
- **Purpose:** Detect sudden changes or trends

### 4.2 `lag1`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Formula:**
```sql
LAG(actual_meur, 1) OVER (PARTITION BY country, heading ORDER BY year)
```
- **Description:** Expenditure from 1 year ago
- **Nullable:** Yes (NULL for first year)
- **Purpose:** Feature for anomaly detection models

### 4.3 `lag2`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Formula:**
```sql
LAG(actual_meur, 2) OVER (PARTITION BY country, heading ORDER BY year)
```
- **Description:** Expenditure from 2 years ago
- **Nullable:** Yes (NULL for first 2 years)
- **Purpose:** Feature for anomaly detection models

### 4.4 `lag3`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Formula:**
```sql
LAG(actual_meur, 3) OVER (PARTITION BY country, heading ORDER BY year)
```
- **Description:** Expenditure from 3 years ago
- **Nullable:** Yes (NULL for first 3 years)
- **Purpose:** Feature for anomaly detection models

### 4.5 `roll_mean_3`
- **Type:** Double
- **Unit:** Million Euros (€M)
- **Formula:**
```sql
ROUND(AVG(actual_meur) OVER (
    PARTITION BY country, heading 
    ORDER BY year
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
), 2)
```
- **Description:** Rolling 3-year average of PREVIOUS 3 years (NOT including current year)
- **Window:** Per country-heading partition
- **Nullable:** Yes (NULL for first 3 years)
- **Purpose:** Smooth baseline for comparison, reduces single-year noise
- **Example:** For year 2010, roll_mean_3 = AVG(2007, 2008, 2009)

---

## 5. Derived Features - Statistical Baselines (2)

### 5.1 `zscore_absorption`
- **Type:** Double (Decimal)
- **Range:** -5.0 to +5.0 (typically -3 to +3)
- **Formula:**
```sql
ROUND(
    (absorption_gap_meur - AVG(absorption_gap_meur) OVER (PARTITION BY country, heading))
    / NULLIF(STDDEV(absorption_gap_meur) OVER (PARTITION BY country, heading), 0),
3)
```
- **Description:** Z-score of absorption gap (within-country-heading standardization)
- **Interpretation:**
  - **|z| < 2:** Normal absorption pattern
  - **|z| > 2:** Statistical anomaly (95% confidence)
  - **|z| > 3:** Extreme anomaly (99.7% confidence)
- **Purpose:** Baseline anomaly detection for H3

### 5.2 `zscore_net`
- **Type:** Double (Decimal)
- **Range:** -5.0 to +5.0
- **Formula:**
```sql
ROUND(
    (net_pct_gni - AVG(net_pct_gni) OVER (PARTITION BY country))
    / NULLIF(STDDEV(net_pct_gni) OVER (PARTITION BY country), 0),
3)
```
- **Description:** Z-score of net position (within-country standardization)
- **Interpretation:**
  - **|z| < 2:** Normal net position for this country
  - **|z| > 2:** Anomalous year (sudden shift in contributor/receiver status)
- **Purpose:** Baseline anomaly detection for H1

---

## 6. Additional Computed Fields

### 6.1 `year_index`
- **Type:** Integer
- **Range:** 0-24
- **Formula:**
```sql
year - 2000
```
- **Description:** Year normalized to 0-based index
- **Purpose:** Numerical feature for ML models (avoids large year values)

---

## Feature Engineering Pipeline

### Stage 1: Raw Data Extraction (openpyxl + pandas)
```
Excel Sheets → parse_tab() → df_exp, df_rev
```

### Stage 2: Schema Validation (PySpark)
```
df_exp, df_rev → createDataFrame() → df_exp_spark, df_rev_spark
```

### Stage 3: Data Quality Audit (PySpark)
```
df_exp_spark → Missing values, Duplicates, Outliers → Quality Report
```

### Stage 4: Revenue Pivot (pandas)
```
df_rev → pivot_table() → df_rev_wide
```

### Stage 5: Feature Engineering (DuckDB SQL)
```
DuckDB Tables:
  - exp_raw (from df_exp)
  - rev_wide (from df_rev_wide)
  - heading_level (aggregated expenditure)
  - total_receipts (NGEU excluded)
  
SQL Transformations:
  → JOIN heading_level + rev_wide + total_receipts
  → Window functions (LAG, AVG OVER)
  → Arithmetic (net_position, absorption_gap, etc.)
  → Statistical (STDDEV, AVG for Z-scores)
  
Output: features table (3,582 rows × 22 columns)
```

### Stage 6: Export (pandas → PySpark → Parquet)
```
df_feat → spark.createDataFrame() → write.parquet()
```

---

## Feature Usage Matrix

| Feature | H1 Analysis | H3 Analysis | Visualization | Export |
|---------|-------------|-------------|---------------|--------|
| year | ✓ | ✓ | ✓ | ✓ |
| country | ✓ | ✓ | ✓ | ✓ |
| heading | | ✓ | ✓ | ✓ |
| actual_meur | | ✓ | ✓ | ✓ |
| earmarked_meur | | ✓ | | ✓ |
| is_ngeu | Filter | Filter | | ✓ |
| is_preliminary | Filter | Filter | | ✓ |
| receipts_meur | ✓ | | ✓ | ✓ |
| total_contributions | ✓ | | | ✓ |
| gni_size | ✓ | | | ✓ |
| net_position_meur | ✓ | | ✓ | ✓ |
| net_pct_gni | ✓ | | ✓ | ✓ |
| absorption_gap_meur | | ✓ | ✓ | ✓ |
| absorption_rate_pct | | ✓ | ✓ | ✓ |
| yoy_change | | | | ✓ |
| lag1 | | ✓ (Model) | | ✓ |
| lag2 | | ✓ (Model) | | ✓ |
| lag3 | | ✓ (Model) | | ✓ |
| roll_mean_3 | | ✓ (Model) | | ✓ |
| year_index | | | | ✓ |
| zscore_absorption | | ✓ (Baseline) | ✓ | ✓ |
| zscore_net | ✓ (Baseline) | | ✓ | ✓ |

---

## Data Quality Constraints

| Feature | Constraint | Validation |
|---------|------------|------------|
| year | 2000 <= year <= 2024 | Assert in parser |
| country | IN (EU28 list) | Check column headers |
| heading | NOT NULL after mapping | Validate MFF_CROSSWALK |
| actual_meur | >= 0 | Business rule check |
| earmarked_meur | >= 0 | Business rule check |
| absorption_rate_pct | 0 <= rate <= 120 | Warning if > 100% |
| net_pct_gni | -10% <= pct <= 5% | Statistical range |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-03-21 | Initial feature definitions for EU Budget pipeline |

---

## References

- MFF 2021-2027 Regulation: https://eur-lex.europa.eu/eli/reg/2020/2093
- EU Budget Structure: https://ec.europa.eu/info/strategy/eu-budget_en
- NGEU Overview: https://ec.europa.eu/info/strategy/recovery-plan-europe_en
