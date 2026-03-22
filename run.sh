#!/bin/bash
# EU Budget Anomaly Detection Pipeline - Execution Script
# File: run.sh
# Purpose: Execute the complete pipeline in Databricks or local environment

set -e  # Exit on error
set -u  # Exit on undefined variable

# ==============================================================================
# CONFIGURATION
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Pipeline configuration
NOTEBOOK_PATH="${NOTEBOOK_PATH:-/Workspace/Users/$(whoami)/EU_Budget_Pipeline_WITH_VISUALIZATIONS}"
CLUSTER_ID="${CLUSTER_ID:-}"
JOB_NAME="EU_Budget_Anomaly_Detection_Pipeline"

# Output paths
LOG_DIR="$PROJECT_ROOT/logs"
OUTPUT_DIR="$PROJECT_ROOT/outputs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"

# ==============================================================================
# FUNCTIONS
# ==============================================================================

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

check_command() {
    if ! command -v $1 &> /dev/null; then
        error "$1 could not be found. Please install it first."
    fi
}

print_banner() {
    echo -e "${BLUE}"
    echo "================================================================================"
    echo "  EU Budget Anomaly Detection Pipeline"
    echo "  Version: 1.0"
    echo "  Execution Time: $(date)"
    echo "================================================================================"
    echo -e "${NC}"
}

# ==============================================================================
# PRE-FLIGHT CHECKS
# ==============================================================================

preflight_checks() {
    log "Running pre-flight checks..."
    
    # Create necessary directories
    mkdir -p "$LOG_DIR"
    mkdir -p "$OUTPUT_DIR"
    
    # Check if running in Databricks or local
    if [ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]; then
        info "Running in Databricks environment (Runtime: $DATABRICKS_RUNTIME_VERSION)"
        ENVIRONMENT="databricks"
    else
        info "Running in local environment"
        ENVIRONMENT="local"
        
        # Check required commands for local execution
        check_command python3
        check_command pip
    fi
    
    # Check Python version
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    info "Python version: $PYTHON_VERSION"
    
    if [[ $(echo "$PYTHON_VERSION 3.10" | awk '{print ($1 < $2)}') -eq 1 ]]; then
        warn "Python version < 3.10. Recommended: 3.10+"
    fi
    
    log "✓ Pre-flight checks complete"
}

# ==============================================================================
# ENVIRONMENT SETUP
# ==============================================================================

setup_environment() {
    log "Setting up environment..."
    
    if [ "$ENVIRONMENT" = "local" ]; then
        # Create virtual environment if it doesn't exist
        if [ ! -d "$PROJECT_ROOT/venv" ]; then
            info "Creating virtual environment..."
            python3 -m venv "$PROJECT_ROOT/venv"
        fi
        
        # Activate virtual environment
        source "$PROJECT_ROOT/venv/bin/activate"
        
        # Install dependencies
        info "Installing dependencies..."
        pip install --quiet --upgrade pip
        pip install --quiet -r "$PROJECT_ROOT/requirements.txt"
        
        log "✓ Environment setup complete"
    else
        log "✓ Using Databricks environment (no setup needed)"
    fi
}

# ==============================================================================
# DATA VALIDATION
# ==============================================================================

validate_data() {
    log "Validating data files..."
    
    if [ "$ENVIRONMENT" = "databricks" ]; then
        # Check Unity Catalog volume
        databricks fs ls dbfs:/Volumes/eu-spending/eu-budget/raw-budget-data/ > /dev/null 2>&1 || \
            error "Cannot access Unity Catalog volume. Check permissions."
        
        info "Unity Catalog volume accessible"
    else
        # Check local data files
        DATA_DIR="$PROJECT_ROOT/data/raw"
        if [ ! -f "$DATA_DIR/eu_budget_spending_and_revenue_2000-2023.xlsx" ]; then
            error "Main data file not found: $DATA_DIR/eu_budget_spending_and_revenue_2000-2023.xlsx"
        fi
        info "Data files found"
    fi
    
    log "✓ Data validation complete"
}

# ==============================================================================
# PIPELINE EXECUTION
# ==============================================================================

run_databricks_job() {
    log "Executing pipeline in Databricks..."
    
    # Check if databricks CLI is available
    check_command databricks
    
    # Get or create job
    JOB_ID=$(databricks jobs list --output JSON | \
             jq -r ".jobs[] | select(.settings.name==\"$JOB_NAME\") | .job_id" | \
             head -1)
    
    if [ -z "$JOB_ID" ]; then
        info "Job not found. Creating new job..."
        JOB_ID=$(create_databricks_job)
    fi
    
    info "Job ID: $JOB_ID"
    
    # Run job
    info "Starting job execution..."
    RUN_ID=$(databricks jobs run-now --job-id "$JOB_ID" --output JSON | jq -r '.run_id')
    info "Run ID: $RUN_ID"
    
    # Monitor job
    monitor_databricks_run "$RUN_ID"
}

create_databricks_job() {
    info "Creating Databricks job..."
    
    JOB_CONFIG=$(cat <<EOF
{
  "name": "$JOB_NAME",
  "tasks": [{
    "task_key": "run_pipeline",
    "notebook_task": {
      "notebook_path": "$NOTEBOOK_PATH",
      "source": "WORKSPACE"
    },
    "new_cluster": {
      "spark_version": "14.3.x-scala2.12",
      "node_type_id": "Standard_DS3_v2",
      "num_workers": 0,
      "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
      },
      "custom_tags": {
        "ResourceClass": "SingleNode"
      }
    },
    "libraries": [
      {"pypi": {"package": "openpyxl==3.1.5"}},
      {"pypi": {"package": "duckdb==1.4.4"}},
      {"pypi": {"package": "scikit-learn==1.7.1"}}
    ]
  }],
  "timeout_seconds": 3600,
  "max_concurrent_runs": 1
}
EOF
)
    
    echo "$JOB_CONFIG" | databricks jobs create --json - | jq -r '.job_id'
}

monitor_databricks_run() {
    local RUN_ID=$1
    local STATUS="PENDING"
    
    info "Monitoring run progress..."
    
    while [[ "$STATUS" != "SUCCESS" && "$STATUS" != "FAILED" && "$STATUS" != "CANCELED" ]]; do
        sleep 10
        STATUS=$(databricks runs get --run-id "$RUN_ID" --output JSON | jq -r '.state.life_cycle_state')
        RESULT=$(databricks runs get --run-id "$RUN_ID" --output JSON | jq -r '.state.result_state // "RUNNING"')
        
        info "Status: $STATUS - Result: $RESULT"
    done
    
    if [ "$STATUS" = "SUCCESS" ]; then
        log "✓ Pipeline completed successfully!"
    else
        error "Pipeline failed with status: $STATUS"
    fi
}

run_local_notebook() {
    log "Executing pipeline locally..."
    
    # Execute the standalone Python script
    NOTEBOOK_FILE="$PROJECT_ROOT/EU_Budget_Pipeline_STANDALONE.py"
    
    if [ ! -f "$NOTEBOOK_FILE" ]; then
        error "Pipeline file not found: $NOTEBOOK_FILE"
    fi
    
    info "Running pipeline..."
    python3 "$NOTEBOOK_FILE" 2>&1 | tee -a "$LOG_FILE"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log "✓ Pipeline completed successfully!"
    else
        error "Pipeline execution failed"
    fi
}

# ==============================================================================
# POST-PROCESSING
# ==============================================================================

collect_outputs() {
    log "Collecting outputs..."
    
    if [ "$ENVIRONMENT" = "databricks" ]; then
        # Download outputs from Unity Catalog
        info "Downloading outputs from Unity Catalog..."
        databricks fs cp -r dbfs:/Volumes/eu-spending/eu-budget/outputs/ "$OUTPUT_DIR/" || \
            warn "Could not download all outputs"
    fi
    
    # Generate summary report
    generate_summary_report
    
    log "✓ Outputs collected"
}

generate_summary_report() {
    local SUMMARY_FILE="$OUTPUT_DIR/pipeline_summary_${TIMESTAMP}.txt"
    
    cat > "$SUMMARY_FILE" <<EOF
EU Budget Anomaly Detection Pipeline - Execution Summary
=========================================================

Execution Time: $(date)
Environment: $ENVIRONMENT
Duration: $SECONDS seconds

Files Generated:
$(ls -lh "$OUTPUT_DIR" 2>/dev/null || echo "No outputs found")

Log File: $LOG_FILE

Status: SUCCESS
EOF
    
    info "Summary report: $SUMMARY_FILE"
}

# ==============================================================================
# CLEANUP
# ==============================================================================

cleanup() {
    log "Cleaning up..."
    
    # Deactivate virtual environment if active
    if [ "$ENVIRONMENT" = "local" ] && [ -n "${VIRTUAL_ENV:-}" ]; then
        deactivate 2>/dev/null || true
    fi
    
    log "✓ Cleanup complete"
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

main() {
    print_banner
    
    # Set trap for cleanup on exit
    trap cleanup EXIT
    
    # Execute pipeline stages
    preflight_checks
    setup_environment
    validate_data
    
    if [ "$ENVIRONMENT" = "databricks" ]; then
        run_databricks_job
    else
        run_local_notebook
    fi
    
    collect_outputs
    
    log "Pipeline execution complete!"
    log "Total time: $SECONDS seconds"
}

# ==============================================================================
# ENTRY POINT
# ==============================================================================

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --notebook-path)
            NOTEBOOK_PATH="$2"
            shift 2
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --notebook-path PATH    Path to notebook in Databricks workspace"
            echo "  --cluster-id ID         Databricks cluster ID (optional)"
            echo "  --help                  Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  NOTEBOOK_PATH           Path to notebook (default: auto-detect)"
            echo "  CLUSTER_ID              Cluster ID for execution"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            ;;
    esac
done

# Run main function
main
