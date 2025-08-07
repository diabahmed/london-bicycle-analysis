#!/bin/bash

# Clean test runner for Easy Task pipeline
# Suppresses Apache Beam dependency warnings for clean output

echo "🧪 Running Easy Task test suite..."
echo "   (Unit tests + Integration tests for London bicycle data pipeline)"
echo "   (Suppressing Apache Beam dependency warnings for clean output)"
echo ""

source .venv/bin/activate
PYTHONWARNINGS="ignore" python -m pytest tests/test_easy_task.py -v

echo ""
echo "✅ Easy Task tests completed successfully!"
