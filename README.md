# EU Budget Anomaly Detection Pipeline

**CRISP-DM Implementation for EU Budget Analysis (2000-2023)**

[![Docker](https://img.shields.io/badge/Docker-Required-blue)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.10-green)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/)

---

## 📊 Project Overview

This pipeline implements a hybrid PySpark + DuckDB architecture to detect anomalies in EU budget data from 2000-2023. It tests two key hypotheses:

**H1 (Net Position Analysis):** Detecting drift in net contributor vs. receiver positions  
**H3 (Absorption Analysis):** Identifying structural funds absorption anomalies using Isolation Forest and LOF

### Key Features
- **Hybrid Architecture:** PySpark for data ingestion, DuckDB for feature engineering
- **22 Engineered Features:** Temporal, ratio-based, and statistical features
- **Dual Anomaly Detection:** Isolation Forest (primary) + Local Outlier Factor (baseline)
- **Comprehensive Auditing:** 97.9% data quality score with detailed audit logs
- **11 Visualizations:** EDA charts + anomaly heatmaps

---

## 🚀 Quick Start

### Prerequisites
- **Docker Desktop** (only requirement)
- 10 GB free disk space
- Internet connection (for initial Docker image build)

### Installation & Execution

```bash
# 1. Clone repository
git clone https://github.com/shardulparulekar/EU_Budget_Pipeline.git
cd EU_Budget_Pipeline

# 2. Run pipeline (one command!)
make run
```

**That's it!** The pipeline will:
1. Build Docker image (~10 minutes first time)
2. Execute full CRISP-DM pipeline (~20 minutes)
3. Generate results in `outputs/` folder

---

## 📁 Repository Structure

```
EU_Budget_Pipeline/
├── EU_Budget_Pipeline_STANDALONE.py    # Main pipeline (1,014 lines)
├── Dockerfile                           # Ubuntu 22.04 + Python 3.10 + Java 11
├── docker-compose.yml                   # Docker orchestration
├── Makefile                             # Simple 'make run' command
├── requirements.txt                     # Python dependencies
├── run.sh                               # Pipeline execution script
├── config/                              # Configuration files
│   └── pipeline_config.ini
├── data/
│   └── raw/
│       └── eu_budget_spending_and_revenue_2000-2023.xlsx  # Embedded in Docker
├── outputs/                             # Generated results (16 files)
│   ├── eu_features.parquet             # 3,582 rows × 22 features
│   ├── h1_net_positions.csv            # 695 country-year observations
│   ├── h3_anomaly_scores.parquet       # 2,984 scored records
│   ├── anomaly_ranked.csv              # Top anomalies ranked
│   ├── pipeline_summary.json           # Execution summary
│   ├── data_quality_audit.json         # Quality metrics (97.9%)
│   └── charts/                         # 11 visualizations
│       ├── eda_a_row_counts.png
│       ├── eda_b_country_coverage.png
│       ├── eda_c_heading_distribution.png
│       ├── eda_d_total_budget.png
│       ├── h1_net_position_drift.png
│       ├── h1_contributors_receivers.png
│       ├── h3_anomaly_heatmap.png
│       ├── h3_absorption_by_heading.png
│       ├── h3_top_10_anomalies.png
│       ├── h3_policy_events.png
│       └── h3_chronic_underabsorbers.png
├── FEATURE_DEFINITIONS.md               # Human-readable feature catalog
└── README.md                            # This file
```

---

## 🔧 Technical Architecture

### Pipeline Stages

**Section 1: Data Ingestion (PySpark)**
- Parse 25 Excel sheets (2000-2024)
- Unified schema across MFF periods
- Data quality audit: 97.9% completeness

**Section 2: Feature Engineering (DuckDB)**
- 22 features across 4 categories:
  - Raw features (8): Direct from source
  - H1 features (5): Net position analysis
  - H3 features (2): Absorption rates
  - Time series (5): Temporal patterns
  - Derived (2): Statistical baselines

**Section 3: H1 Net Position Analysis (PySpark)**
- Window functions for drift detection
- Z-score normalization (threshold: 2.0)
- 27 anomalies detected

**Section 4: H3 Absorption Anomaly Detection (Scikit-learn)**
- **Primary:** Isolation Forest (contamination=0.05)
- **Baseline:** Local Outlier Factor (n_neighbors=20)
- 140 anomalies detected (4.7% of data)

**Section 5: Export & Visualization (Matplotlib)**
- 11 charts at 150 DPI
- Headless rendering (matplotlib.use('Agg'))

### Technology Stack
- **PySpark 3.5.0:** Distributed data processing
- **DuckDB 1.4.4:** In-process analytical queries
- **scikit-learn 1.7.1:** Machine learning (Isolation Forest, LOF)
- **Pandas 2.3.1:** Data manipulation
- **Matplotlib 3.8.2:** Visualization
- **Docker:** Reproducible environment

---

## 📊 Expected Outputs

After running `make run`, you'll find 16 files in `outputs/`:

### Data Files (6)
1. `eu_features.parquet` - 3,582 rows × 22 features
2. `h1_net_positions.csv` - Net position analysis results
3. `h3_anomaly_scores.parquet` - Anomaly scores + predictions
4. `anomaly_ranked.csv` - Top anomalies sorted by severity
5. `pipeline_summary.json` - Execution metrics
6. `data_quality_audit.json` - Data quality report

### Visualizations (11)
- **EDA:** Row counts, country coverage, heading distribution, budget trends
- **H1:** Net position drift, contributors vs receivers
- **H3:** Anomaly heatmap, absorption by heading, top 10 anomalies, policy events, chronic underabsorbers

---

## 🧪 Data Processing Artifacts

### DuckDB Configuration
Embedded in pipeline code (Section 2, lines 400-550):
- Connection: In-memory mode
- Feature SQL queries with CTEs
- Efficient columnar processing

### Spark Configuration
```python
SparkSession.builder \
    .appName("EU Budget Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### Data Quality Audit
Output: `outputs/data_quality_audit.json`

**Metrics:**
- Overall quality score: 97.9%
- Total expenditure rows: 4,117
- Total revenue rows: 4,876
- Missing value rate: 2.1%
- Schema validation: PASS
- Temporal coverage: 25 years (2000-2024)

### Feature Definitions
See `FEATURE_DEFINITIONS.md` for complete catalog of 22 features with:
- Feature name and type
- Calculation formula
- Business interpretation
- Data quality notes

---

## 🐛 Troubleshooting

### Docker Build Fails
```bash
# Clean Docker and rebuild
docker system prune -a --volumes -f
docker build --no-cache -t eu-budget-pipeline .
make run
```

### Pipeline Execution Errors
Check logs in `outputs/logs/` or container output.

### Missing Outputs
Verify Docker volume mount:
```bash
ls -lh outputs/
```

---

## 📚 Documentation

- **Feature Catalog:** `FEATURE_DEFINITIONS.md`
- **Setup Guide:** `GITHUB_SETUP_GUIDE.md`
- **Pipeline Code:** `EU_Budget_Pipeline_STANDALONE.py` (extensively commented)

---

## 📧 Contact

**Project:** EU Budget Anomaly Detection Pipeline  
**Course:** Executive MBA Big Data Infrastructures 2025-2026  
**Repository:** https://github.com/shardulparulekar/EU_Budget_Pipeline

---

## 📄 License

Academic project for educational purposes.

---

**Built with ❤️ using CRISP-DM methodology**
