#!/bin/bash
set -e

echo "=========================================="
echo "Container starting at $(date)"
echo "=========================================="
echo "Python version: $(python --version)"
echo "Working directory: $(pwd)"
echo "Files in /app:"
ls -la /app
echo "=========================================="
echo "Environment variables:"
env | sort
echo "=========================================="
echo "Checking Python packages:"
pip list
echo "=========================================="
echo "Starting truck_activity_simulator.py..."
echo "=========================================="

# Run the Python script with unbuffered output
exec python -u truck_activity_simulator.py
