FROM python:3.9-slim

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install DuckDB with Python bindings
RUN pip install --no-cache-dir \
    duckdb==0.9.2 \
    python-dotenv \
    pandas \
    jupyterlab \
    ipython \
    ipykernel \
    matplotlib \
    boto3 \
    s3fs

# Create workspace directory
WORKDIR /workspace

# Copy startup script
COPY startup.sh /workspace/startup.sh
RUN chmod +x /workspace/startup.sh

# Copy initialization scripts and notebooks
COPY scripts/ /workspace/scripts/
COPY notebooks/ /workspace/notebooks/

# Create s3 directory to mount data
RUN mkdir -p /workspace/data

# Expose the JupyterLab port
EXPOSE 8888

# Set environment variables for Jupyter
ENV JUPYTER_PATH=/root/.local/share/jupyter/
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime/
ENV JUPYTER_DATA_DIR=/root/.local/share/jupyter/

# Use our custom startup script
CMD ["/workspace/startup.sh"] 