#!/bin/bash
echo ""
echo "================================================================================"
echo "  EU Budget Anomaly Detection Pipeline"
echo "  Execution Time: $(date)"
echo "================================================================================"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting pipeline..."
python3 EU_Budget_Pipeline_STANDALONE.py
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Pipeline successful"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✗ Pipeline failed"
    exit $EXIT_CODE
fi
