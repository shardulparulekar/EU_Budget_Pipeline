# Dockerfile for EU Budget Anomaly Detection Pipeline
# Base image: Python 3.10 with Java (required for PySpark)

FROM python:3.10-slim

# Metadata
LABEL maintainer="EU Budget Pipeline"
LABEL version="1.0"
LABEL description="Reproducible environment for EU Budget Anomaly Detection Pipeline"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Java (required for PySpark)
    openjdk-17-jre-headless \
    # Build tools
    gcc \
    g++ \
    make \
    # Utilities
    curl \
    wget \
    git \
    vim \
    # For Excel processing
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    # Cleanup
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy project files
COPY EU_Budget_Pipeline_STANDALONE.py /app/
COPY config/ /app/config/
COPY run.sh /app/

# ============================================================================
# EMBED DATA IN DOCKER IMAGE
# ============================================================================
# Data file is included in the image - teacher doesn't need to provide it!
# The Excel file will be baked into the Docker image
COPY data/raw/eu_budget_spending_and_revenue_2000-2023.xlsx /app/data/raw/

# Create necessary directories
RUN mkdir -p /app/data/raw \
    /app/data/processed \
    /app/outputs \
    /app/logs \
    /app/config

# Set permissions
RUN chmod +x /app/run.sh

# Expose volume for outputs (results will be written here)
VOLUME ["/app/outputs"]

# Default command - runs the pipeline
CMD ["/app/run.sh"]
