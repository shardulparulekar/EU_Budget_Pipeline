# Makefile for EU Budget Pipeline
# Provides convenient commands for building, running, and managing the pipeline

.PHONY: help build run test clean docker-build docker-run docker-shell

# Default target
help:
	@echo "EU Budget Anomaly Detection Pipeline - Make Commands"
	@echo ""
	@echo "Available commands:"
	@echo "  make build           - Build Docker image"
	@echo "  make run             - Run pipeline in Docker"
	@echo "  make shell           - Open shell in Docker container"
	@echo "  make test            - Run tests"
	@echo "  make clean           - Clean outputs and temporary files"
	@echo "  make install         - Install dependencies locally"
	@echo "  make validate-data   - Validate data files"
	@echo "  make docker-build    - Build Docker image (alias for build)"
	@echo "  make docker-run      - Run Docker container (alias for run)"
	@echo ""

# Build Docker image
build:
	@echo "Building Docker image..."
	docker-compose build
	@echo "✓ Docker image built successfully"

# Run pipeline in Docker (data is embedded - no setup needed!)
run:
	@echo "Running pipeline in Docker (data included)..."
	docker-compose up --build
	@echo "✓ Pipeline execution complete"
	@echo "✓ Check outputs/ directory for results"

# Open interactive shell in Docker
shell:
	@echo "Opening shell in Docker container..."
	docker-compose run --rm eu-budget-pipeline /bin/bash

# Install dependencies locally (without Docker)
install:
	@echo "Installing dependencies..."
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "✓ Dependencies installed"

# Run pipeline locally (without Docker)
run-local:
	@echo "Running pipeline locally..."
	./run.sh
	@echo "✓ Pipeline execution complete"

# Clean outputs and temporary files
clean:
	@echo "Cleaning outputs and temporary files..."
	rm -rf outputs/*
	rm -rf logs/*
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf venv
	docker-compose down -v 2>/dev/null || true
	@echo "✓ Cleanup complete"

# Validate embedded data
validate-data:
	@echo "Validating embedded data..."
	@echo "✓ Data is embedded in Docker image (no validation needed)"
	@echo "  To verify, check: data/raw/eu_budget_spending_and_revenue_2000-2023.xlsx"

# Run tests
test:
	@echo "Running tests..."
	pytest tests/ -v --cov=.
	@echo "✓ Tests complete"

# Docker aliases
docker-build: build
docker-run: run
docker-shell: shell

# View logs
logs:
	@echo "Recent logs:"
	@tail -n 50 logs/*.log 2>/dev/null || echo "No logs found"

# Show pipeline status
status:
	@echo "Pipeline Status:"
	@echo "- Docker image: $$(docker images eu-budget-pipeline:latest -q 2>/dev/null || echo 'Not built')"
	@echo "- Output files: $$(ls -1 outputs/ 2>/dev/null | wc -l) files"
	@echo "- Log files: $$(ls -1 logs/ 2>/dev/null | wc -l) files"
