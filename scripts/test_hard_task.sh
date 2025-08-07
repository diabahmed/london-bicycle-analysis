#!/bin/bash

# Clean test runner for Hard Task pipeline
# Suppresses Apache Beam dependency warnings for clean output

echo "🧪 Running Hard Task test suite..."
echo "   (Unit tests + Integration tests for London bicycle data pipeline)"
echo "   (Suppressing Apache Beam dependency warnings for clean output)"
echo ""

source .venv/bin/activate
PYTHONWARNINGS="ignore" python -m pytest tests/test_hard_task.py -v

echo ""
echo "✅ Hard Task tests completed successfully!"
