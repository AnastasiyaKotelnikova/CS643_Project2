# ===== CS643 Project 2 - Starter Dockerfile =====
# This container will later be used for the inference application
# (model prediction API or CLI). For now it's a minimal Python + PySpark
# environment that we will extend once inference_app.py is ready.

# Base image with Python
FROM python:3.10-slim

# Work directory inside the container
WORKDIR /app

# Avoid Python .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Java (required by PySpark) and clean up apt cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (needed by Spark/PySpark)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Install Python dependencies (we will refine this list later)
RUN pip install --no-cache-dir pyspark flask

# Copy inference application into the container
# (in a later step we will create inference/inference_app.py)
COPY ./inference/inference_app.py /app/inference_app.py

# Default command: run the inference app
CMD ["python", "inference_app.py"]
