#!/bin/bash

# Easy Task Runner Script for London Bicycles Data Pipeline
# This script runs the easy task pipeline to calculate ride counts
# for all station pair combinations in the London bicycles dataset.

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required arguments are provided
if [ $# -lt 2 ]; then
    print_error "Usage: $0 <project_id> <bucket_name> [region] [runner]"
    print_error "Example: $0 my-project-id my-bucket-name europe-west10 DataflowRunner"
    exit 1
fi

# Set variables from arguments
PROJECT_ID="$1"
BUCKET_NAME="$2"
REGION="${3:-europe-west10}"  # Default to europe-west10

print_status "Running Easy Task Pipeline on Google Cloud Dataflow..."
print_status "Project ID: $PROJECT_ID"
print_status "Bucket: $BUCKET_NAME"
print_status "Region: $REGION"

# Validate project ID format
if [[ ! "$PROJECT_ID" =~ ^[a-z][a-z0-9\-]{4,28}[a-z0-9]$ ]]; then
    print_warning "Project ID format may be invalid. Continuing anyway..."
fi

# Check if gcloud CLI is available
if ! command -v gcloud &> /dev/null; then
    print_warning "gcloud CLI not found. Pipeline might fail if authentication is not set up properly."
fi

# Validate that we're in the correct directory
if [ ! -f "setup.py" ]; then
    print_error "setup.py not found. Please run this script from the project root directory."
    exit 1
fi

# Validate that the source files exist
if [ ! -f "src/pipeline/easy_task_pipeline.py" ]; then
    print_error "Hard task pipeline file not found: src/pipeline/easy_task_pipeline.py"
    exit 1
fi

if [ ! -f "src/transforms/easy_task.py" ]; then
    print_error "Distance transforms file not found: src/transforms/easy_task.py"
    exit 1
fi

# Check if bucket exists (optional validation)
if command -v gsutil &> /dev/null; then
    if ! gsutil ls "gs://$BUCKET_NAME" &> /dev/null; then
        print_warning "Cannot access bucket gs://$BUCKET_NAME or gsutil not configured"
        print_warning "Make sure the bucket exists and you have proper permissions"
    fi
fi

# Clean up any existing output files in the bucket
print_status "Cleaning up previous output files..."
if command -v gsutil &> /dev/null; then
    gsutil -m rm -rf "gs://$BUCKET_NAME/output/results*" 2>/dev/null || true
fi

# Set environment variables for warning suppression
export PYTHONWARNINGS="ignore"
export BEAM_LOG_LEVEL="INFO"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    print_status "Activating virtual environment..."
    source .venv/bin/activate
else
    print_warning "Virtual environment not found. Using system Python."
fi

# Run the pipeline
print_status "Starting Easy Task Pipeline execution..."

python main.py --task easy \
    --project="$PROJECT_ID" \
    --bucket="$BUCKET_NAME" \
    --region="$REGION" \

# Check if the pipeline completed successfully
if [ $? -eq 0 ]; then
    print_success "Easy Task Pipeline completed successfully!"
    print_success "Results written to gs://$BUCKET_NAME/output/"
    print_status "Output format: start_station_id,end_station_id,ride_count"
    
    # Display some information about the output
    if command -v gsutil &> /dev/null; then
        print_status "Checking output files..."
        gsutil ls "gs://$BUCKET_NAME/output/results*" 2>/dev/null || print_warning "Could not list output files"
        
        # Show a sample of the results
        print_status "Sample results (first 5 lines):"
        gsutil cat "gs://$BUCKET_NAME/output/results.txt" 2>/dev/null | head -5 || print_warning "Could not display sample results"
    fi
    
    print_status "Pipeline submitted! Check gs://$BUCKET_NAME/output/ for results."
else
    print_error "Easy Task Pipeline failed with exit code $?"
    exit 1
fi
