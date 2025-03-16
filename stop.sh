#!/bin/bash

# Data Engineering Architecture Shutdown Script
echo "Stopping Data Engineering Architecture..."

# Stop all services
docker-compose down

echo "All services have been stopped."
echo "Data in volumes (postgres-db-volume and minio-data) is preserved."
echo "To remove volumes as well, run: docker-compose down -v" 