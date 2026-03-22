# Docker Reproducibility Guide
## EU Budget Anomaly Detection Pipeline

This directory contains a complete Docker setup for reproducible execution of the EU Budget pipeline.

---

## 📦 What's Included

```
.
├── Dockerfile                              # Container definition
├── docker-compose.yml                      # Service orchestration
├── requirements.txt                        # Python dependencies
├── run.sh                                  # Execution script
├── Makefile                                # Convenience commands
├── config/
│   ├── duckdb_config.ini                   # DuckDB settings
│   └── spark_config.conf                   # Spark configuration
├── data/
│   └── raw/                                # Place Excel files here
├── outputs/                                # Pipeline outputs (created)
└── logs/                                   # Execution logs (created)
```

---

## 🚀 Quick Start (3 Commands)

### 1. Build the Docker Image
```bash
make build
# OR
docker-compose build
```

**Time:** ~5 minutes  
**Size:** ~2GB

### 2. Place Your Data
```bash
# Copy Excel files to data/raw/
cp path/to/eu_budget_spending_and_revenue_2000-2023.xlsx data/raw/
```

### 3. Run the Pipeline
```bash
make run
# OR
docker-compose run --rm eu-budget-pipeline /app/run.sh
```

**Runtime:** ~15-20 minutes  
**Outputs:** `outputs/` directory

---

## 🛠️ Available Commands

### Using Makefile (Recommended):

```bash
make build           # Build Docker image
make run             # Run pipeline
make shell           # Open interactive shell
make clean           # Clean outputs and temp files
make validate-data   # Check data files exist
make logs            # View recent logs
make status          # Show pipeline status
```

### Using Docker Compose Directly:

```bash
# Build image
docker-compose build

# Run pipeline
docker-compose run --rm eu-budget-pipeline /app/run.sh

# Interactive shell
docker-compose run --rm eu-budget-pipeline /bin/bash

# Clean up
docker-compose down -v
```

### Using Docker Directly:

```bash
# Build image
docker build -t eu-budget-pipeline:latest .

# Run container
docker run --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/outputs:/app/outputs \
    -v $(pwd)/logs:/app/logs \
    eu-budget-pipeline:latest \
    /app/run.sh

# Interactive shell
docker run -it --rm \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/outputs:/app/outputs \
    eu-budget-pipeline:latest \
    /bin/bash
```

---

## 📂 Directory Structure

### Before Running:
```
project/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run.sh
├── config/
│   ├── duckdb_config.ini
│   └── spark_config.conf
└── data/
    └── raw/
        └── eu_budget_spending_and_revenue_2000-2023.xlsx
```

### After Running:
```
project/
├── ... (same as above)
├── outputs/
│   ├── eu_features.parquet
│   ├── h1_net_positions.csv/
│   ├── h3_anomaly_scores.parquet
│   ├── anomaly_ranked.csv/
│   ├── pipeline_summary.json
│   └── charts/
│       ├── eda_a_row_counts.png
│       ├── h1_net_position_drift.png
│       └── ... (11 charts total)
└── logs/
    └── pipeline_20260321_123456.log
```

---

## 🔧 Configuration

### Environment Variables

Create `.env` file (optional):
```bash
# .env file
CATALOG_SCHEMA=eu-spending
VOLUME_NAME=eu-budget
OUTPUT_FOLDER=outputs
```

### Resource Limits

Edit `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      cpus: '4'        # Adjust based on your system
      memory: 8G       # Adjust based on your system
```

---

## 🐛 Troubleshooting

### Issue: "Cannot allocate memory"

**Solution:** Increase Docker memory limit

**Mac/Windows (Docker Desktop):**
1. Docker Desktop → Settings → Resources
2. Increase Memory to 8GB+
3. Apply & Restart

**Linux:**
```bash
# No limit on native Linux Docker
```

### Issue: "Permission denied" on run.sh

**Solution:**
```bash
chmod +x run.sh
```

### Issue: "Data file not found"

**Solution:**
```bash
# Check files are in correct location
ls -la data/raw/

# Should show:
# eu_budget_spending_and_revenue_2000-2023.xlsx
```

### Issue: "Port already in use"

**Solution:**
```bash
# Not applicable (pipeline doesn't use ports)
```

---

## 🔍 Inspecting Results

### View Summary
```bash
cat outputs/pipeline_summary.json
```

### View Logs
```bash
tail -f logs/pipeline_*.log
```

### Access Files
```bash
# All outputs in outputs/ directory
ls -lh outputs/
```

---

## 📊 Expected Outputs

After successful execution:

| File | Description | Size |
|------|-------------|------|
| `eu_features.parquet` | Master feature table (3,582 rows × 22 features) | ~2MB |
| `h1_net_positions.csv/` | H1 net position analysis | ~100KB |
| `h3_anomaly_scores.parquet` | H3 anomaly scores | ~800KB |
| `anomaly_ranked.csv/` | Top 30 anomalies | ~5KB |
| `pipeline_summary.json` | Execution summary | <1KB |
| `charts/*.png` | 11 visualization charts | ~5MB total |

---

## 🔒 Reproducibility Guarantees

### Fixed Versions
- **Python:** 3.10
- **Java:** 17 (for PySpark)
- **Libraries:** Pinned in requirements.txt
  - openpyxl==3.1.5
  - pandas==2.3.1
  - duckdb==1.4.4
  - pyspark==3.5.0
  - scikit-learn==1.7.1

### Random Seeds
All ML algorithms use fixed seeds:
```python
IsolationForest(random_state=42)
```

### Deterministic Execution
- SQL queries are deterministic
- Window functions use fixed ordering
- No timestamp-based features

---

## 🎯 Deployment Scenarios

### Scenario 1: Local Development
```bash
make shell
# Inside container:
python3 EU_Budget_Pipeline_WITH_VISUALIZATIONS.py
```

### Scenario 2: Automated CI/CD
```yaml
# .github/workflows/pipeline.yml
jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: make build
      - run: make run
      - uses: actions/upload-artifact@v3
        with:
          path: outputs/
```

### Scenario 3: Production Server
```bash
# Run as background job
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

---

## 📝 Modifying the Pipeline

### 1. Edit Notebook
```bash
# Edit on host
vim EU_Budget_Pipeline_WITH_VISUALIZATIONS.py

# Rebuild image
make build

# Test changes
make run
```

### 2. Change Configuration
```bash
# Edit config files
vim config/duckdb_config.ini
vim config/spark_config.conf

# No rebuild needed (mounted as volume)
make run
```

### 3. Add Dependencies
```bash
# Edit requirements.txt
echo "new-package==1.0.0" >> requirements.txt

# Rebuild image
make build
```

---

## 🧪 Testing

### Run Tests
```bash
make test
```

### Manual Testing
```bash
make shell

# Inside container:
pytest tests/ -v
```

---

## 🗑️ Cleanup

### Remove Outputs
```bash
make clean
```

### Remove Docker Image
```bash
docker rmi eu-budget-pipeline:latest
```

### Full Cleanup
```bash
make clean
docker-compose down -v
docker system prune -a
```

---

## 📚 References

- **Docker Documentation:** https://docs.docker.com/
- **Docker Compose:** https://docs.docker.com/compose/
- **PySpark in Docker:** https://spark.apache.org/docs/latest/

---

## ✅ Reproducibility Checklist

For your project submission:

- [ ] Dockerfile builds successfully
- [ ] All dependencies pinned in requirements.txt
- [ ] Data files documented (where to obtain)
- [ ] Configuration files included
- [ ] README provides clear instructions
- [ ] Pipeline runs end-to-end without errors
- [ ] Outputs match expected results
- [ ] Random seeds fixed (ML reproducibility)
- [ ] Execution time documented (~15-20 min)
- [ ] System requirements documented (8GB RAM, 4 CPU cores)

---

**Your pipeline is now fully reproducible!** 🎉

Anyone can clone the repository, run `make build && make run`, and get identical results.
