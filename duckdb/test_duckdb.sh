#!/bin/bash
# This script helps to test the DuckDB integration from outside the container

# Ensure the services are running
docker-compose ps duckdb minio

if [ $? -ne 0 ]; then
  echo "Services are not running. Please start them first with 'docker-compose up -d duckdb minio'"
  exit 1
fi

echo "Testing DuckDB connection to MinIO..."
docker-compose exec duckdb python /workspace/notebooks/test_duckdb.py

if [ $? -eq 0 ]; then
  echo
  echo "DuckDB test completed successfully!"
  echo "You can now access JupyterLab at http://localhost:8888"
  echo "or run Python scripts like the test script we just executed."
else
  echo
  echo "DuckDB test failed. Please check the error message above for details."
fi 