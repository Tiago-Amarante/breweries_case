#!/bin/bash
set -e

# Make sure the Python kernel is properly installed
python -m ipykernel install --name=python3 --display-name="Python 3" --user

# Check if the kernel is properly installed
jupyter kernelspec list

# Start JupyterLab
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' 