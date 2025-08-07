# London Bicycles Analysis

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.66.0-orange.svg)](https://beam.apache.org/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Dataflow-4285f4.svg)](https://cloud.google.com/dataflow)
[![BigQuery](https://img.shields.io/badge/BigQuery-Public%20Dataset-4285f4.svg)](https://cloud.google.com/bigquery)
[![Geopy](https://img.shields.io/badge/Geopy-2.4.1-green.svg)](https://geopy.readthedocs.io/)
[![Tests](https://img.shields.io/badge/Tests-12%20Passing-brightgreen.svg)](#-testing)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

This project implements Apache Beam pipelines for analyzing London bicycle usage data using Google Cloud Dataflow and BigQuery. The project includes both **Easy Task** (ride counting) and **Hard Task** (distance calculations) implementations.

## 🎯 Challenge Overview

**Easy Task**: Count rides between station pairs  
**Hard Task**: Calculate total distance covered for each station pair

**Data Source**: BigQuery public dataset `bigquery-public-data.london_bicycles`

- `cycle_hire` table: Individual bicycle rental records
- `cycle_stations` table: Station location information

**Outputs**:

- Easy Task: `start_station_id,end_station_id,ride_count`
- Hard Task: `start_station_id,end_station_id,ride_count,total_distance`

---

## 🚀 Quick Start (From Scratch)

### Prerequisites

1. **Google Cloud Project** with:
   - Dataflow API enabled
   - BigQuery API enabled
   - Cloud Storage API enabled
   - Cloud Build API enabled
2. **Google Cloud Storage bucket** for pipeline outputs
3. **Python 3.11+** installed
4. **gcloud CLI** installed and authenticated

### 1. Setup Authentication & Project

```bash
# Authenticate with Google Cloud
gcloud auth application-default login

# Set your project (replace with your actual project ID)
export GOOGLE_CLOUD_PROJECT="your-project-id"
gcloud config set project $GOOGLE_CLOUD_PROJECT

# Enable required APIs
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### 2. Create Cloud Storage Bucket

```bash
# Create bucket (replace with unique bucket name)
export BUCKET_NAME="your-unique-bucket-name"
gsutil mb gs://$BUCKET_NAME
```

### 3. Setup Python Environment

```bash
# Clone/download this repository
git clone https://github.com/diabahmed/london-bicycle-analysis.git
cd london-bicycle-analysis

# Create and activate virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e .

# For testing capabilities, install dev dependencies
pip install -e ."[dev]"
```

### 4. Run Tests (Recommended)

```bash
# Run comprehensive test suites
./scripts/test_easy_task.sh
./scripts/test_hard_task.sh
```

### 5. Deploy Pipelines to Google Cloud Dataflow

```bash
# Run the Easy Task pipeline (ride counting)
./scripts/run_easy_task.sh $GOOGLE_CLOUD_PROJECT $BUCKET_NAME

# Run the Hard Task pipeline (distance calculations)
./scripts/run_hard_task.sh $GOOGLE_CLOUD_PROJECT $BUCKET_NAME

# Or run directly with Python
python main.py --task easy --project $GOOGLE_CLOUD_PROJECT --bucket $BUCKET_NAME
python -m src.pipeline.hard_task_pipeline --project $GOOGLE_CLOUD_PROJECT --bucket $BUCKET_NAME
```

### 6. Check Results

```bash
# View Easy/Hard Task output
gsutil cat gs://$BUCKET_NAME/output/results.txt

# Monitor jobs in Google Cloud Console
# Navigate to: Dataflow > Jobs
```

---

## 📁 Project Architecture

```
london-bicycle-analysis/
├── 📄 main.py                          # 🚪 Entry point - start pipeline
├── 📄 config.py                        # ⚙️ Pipeline configuration
├── 📄 setup.py                         # 📦 Dependencies & package info
├── 📂 scripts/
│   ├── 📄 test_easy_task.sh            # 🧪 Easy task test runner
│   ├── 📄 test_hard_task.sh            # 🧪 Hard task test runner
│   ├── 📄 run_easy_task.sh             # 🚀 Easy task deployment script
│   └── 📄 run_hard_task.sh             # 🚀 Hard task deployment script
├── 📂 src/
│   ├── 📄 __init__.py
│   ├── 📄 models.py                    # 📊 Data models (StationRideCount, StationDistanceResult)
│   ├── 📄 queries.py                   # 🔍 BigQuery SQL queries (Easy & Hard tasks)
│   ├── 📂 pipeline/
│   │   ├── 📄 __init__.py
│   │   ├── 📄 easy_task_pipeline.py    # 🏗️ Easy task pipeline orchestration
│   │   └── 📄 hard_task_pipeline.py    # 🏗️ Hard task pipeline orchestration
│   └── 📂 transforms/
│       ├── 📄 __init__.py
│       ├── 📄 easy_task.py             # 🔄 Easy task Apache Beam transforms
│       └── 📄 hard_task.py             # 🔄 Hard task Apache Beam transforms (with geopy)
└── 📂 tests/
    ├── 📄 test_easy_task.py            # ✅ Easy task unit & integration tests
    └── 📄 test_hard_task.py            # ✅ Hard task unit & integration tests
```

---

## 🏗️ Technical Implementation

### Easy Task Pipeline Flow

```
BigQuery → Validate → Group → Count → Format → Cloud Storage
    ↓         ↓        ↓       ↓        ↓          ↓
Raw Data  Filter   Station  Count   CSV Lines  results.txt
         Invalid    Pairs   Rides   Format
         Records
```

### Hard Task Pipeline Flow

```
BigQuery (Rides) ──┐
                   ├── Join → Calculate → Format → Cloud Storage
BigQuery (Stations)─┘      Distance                    ↓
    ↓                         ↓                  results.txt
Station Coords        Geopy Distance          CSV with total_distance
(lat, lon)            Calculations
```

#### Easy Task Steps:

1. **📊 Data Extraction** (`queries.py`)

   ```sql
   SELECT start_station_id, end_station_id
   FROM bigquery-public-data.london_bicycles.cycle_hire
   ```

2. **🔍 Data Validation** (`ExtractValidRides`)

   - Filters out records with `NULL` or same station IDs
   - Returns valid (start_station_id, end_station_id) pairs

3. **👥 Grouping & Counting** (`CountRidesByStationPair`)

   - Groups by station pair: `(start_station_id, end_station_id)`
   - Counts rides for each unique pair using `StationRideCount` model

4. **📝 Formatting** (`FormatRideCountOutput`)
   - Converts to CSV format: `"1,2,150"`

#### Hard Task Steps:

1. **📊 Dual Data Extraction** (`queries.py`)

   ```sql
   -- Ride data
   SELECT start_station_id, end_station_id
   FROM bigquery-public-data.london_bicycles.cycle_hire

   -- Station location data
   SELECT id, latitude, longitude
   FROM bigquery-public-data.london_bicycles.cycle_stations
   ```

2. **🗺️ Location Processing** (`ExtractStationLocations`)

   - Extracts station coordinates as (station_id, (lat, lon))
   - Creates side input for distance calculations

3. **📏 Distance Calculation** (`CalculateDistanceAndFormat`)

   - Uses `geopy.distance.geodesic` for Haversine formula
   - Calculates straight-line distance between station coordinates
   - Multiplies by ride count for total distance
   - Creates `StationDistanceResult` model objects

4. **📝 Formatting** (`FormatDistanceOutput`)
   - Converts to CSV: `"1,2,150,2.456789"`

### Key Components Explained

#### 🎯 `main.py` - Main Entry Point

```python
# Handles command-line arguments and starts the Easy/Hard Task pipeline
python main.py --task easy/hard --project PROJECT --bucket BUCKET
```

#### ⚙️ `config.py` - Configuration

```python
DEFAULT_DATAFLOW_OPTIONS = {
    'save_main_session': True,
    'use_public_ips': True,
    'machine_type': 'e2-standard-2',
    # Prebuild SDK container to avoid runtime dependency installation
    'experiments': ['use_runner_v2'],
    'setup_file': './setup.py',
    'prebuild_sdk_container_engine': 'cloud_build',
    'docker_registry_push_url': '{region}-docker.pkg.dev/{project}/dataflow-containers/prebuilt-sdk',
    'sdk_location': 'container',
}
```

#### 📊 `models.py` - Enhanced Data Structures

```python
class StationRideCount(NamedTuple):
    """Easy task results model."""
    start_station_id: int
    end_station_id: int
    ride_count: int

class StationDistanceResult(NamedTuple):
    """Hard task results model."""
    start_station_id: int
    end_station_id: int
    ride_count: int
    total_distance: float
```

#### 🔍 `queries.py` - Centralized SQL Queries

```python
def get_cycle_hire_query() -> str:
    """Basic ride data extraction."""

def get_cycle_stations_query() -> str:
    """Station information extraction."""
```

#### 🔄 `transforms/easy_task.py` - Easy Task Transforms

```python
class ExtractValidRides(beam.DoFn):
    """Filters out invalid station pairs"""

class CountRidesByStationPair(beam.DoFn):
    """Counts rides per station pair, creates StationRideCount objects"""

class FormatRideCountOutput(beam.DoFn):
    """Formats StationRideCount objects as CSV"""
```

#### 🔄 `transforms/hard_task.py` - Hard Task Transforms

```python
class ExtractValidRidesForDistance(beam.DoFn):
    """Filters rides for distance calculations (excludes same-station rides)"""

class ExtractStationLocations(beam.DoFn):
    """Extracts station coordinates from station data"""

class CountRidesByStationPairForDistance(beam.DoFn):
    """Counts rides per station pair for distance calculations"""

class CalculateDistance(beam.DoFn):
    """Calculates geodesic distance using geopy, creates StationDistanceResult objects"""

class FormatDistanceOutput(beam.DoFn):
    """Formats StationDistanceResult objects as CSV with distance"""
```

---

## 🧪 Testing

### Test Structure

- **Unit Tests**: Test individual transforms in isolation using Apache Beam TestPipeline
- **Integration Tests**: Test complete pipelines with sample data
- **Mock Data**: Simulates BigQuery responses for local testing
- **Geopy Integration Tests**: Validates distance calculations with known coordinates

### Running Tests

```bash
# Clean output with colored status (recommended)
./scripts/test_easy_task.sh
./scripts/test_hard_task.sh

# Verbose output with details
source .venv/bin/activate
python -m pytest tests/ -v

# Run specific test files
python -m pytest tests/test_easy_task.py -v
python -m pytest tests/test_hard_task.py -v

# Run specific test
python -m pytest tests/test_easy_task.py::TestEasyTaskTransforms::test_extract_valid_rides -v
```

### Test Coverage

#### Easy Task Tests ✅

- `ExtractValidRides` - Validates filtering logic for valid station pairs
- `CountRidesByStationPair` - Validates counting logic and StationRideCount creation
- `FormatRideCountOutput` - Validates CSV formatting from model objects
- `build_easy_task_pipeline` - End-to-end integration test

#### Hard Task Tests ✅

- `ExtractValidRidesForDistance` - Validates filtering (excludes same-station rides)
- `ExtractStationLocations` - Validates station coordinate extraction
- `CountRidesByStationPairForDistance` - Validates ride counting for distance calculations
- `CalculateDistanceAndFormat` - Validates geopy distance calculations and StationDistanceResult creation
- `FormatDistanceOutput` - Validates CSV formatting with distance data
- `build_hard_task_pipeline` - End-to-end integration test with dual BigQuery inputs
- `GeopyIntegration` - Validates geopy accuracy with known London landmarks

---

## 🔧 Development Setup

### For Development Work

```bash
# Install with development dependencies
pip install -e ."[dev]"

# This includes:
# - pytest (testing framework)
# - All production dependencies
```

### Project Dependencies

**Production** (`setup.py`):

- `apache-beam[gcp]==2.66.0` - Latest data processing framework
- `geopy==2.4.1` - Geographic distance calculations (Haversine formula)

**Development** (`setup.py` extras):

- `pytest` - Testing framework

### Enhanced Script Features

Both runner scripts (`run_easy_task.sh` and `run_hard_task.sh`) now include:

- **🎨 Colored Output**: Status messages with color coding
- **🔍 Input Validation**: Project ID and bucket name format validation
- **🛠️ Environment Checks**: Virtual environment and gcloud CLI detection
- **🧹 Cleanup**: Automatic removal of previous output files
- **📊 Results Display**: Sample output preview after completion
- **⚠️ Error Handling**: Comprehensive error reporting and exit codes

---

## 🌍 Google Cloud Configuration

### Required APIs

- **Dataflow API** - For pipeline execution
- **BigQuery API** - For data access
- **Cloud Storage API** - For output storage

### Recommended Regions

- `europe-west10` (Berlin) - Default for this project
- `europe-west1` (Belgium) - Alternative EU region
- `us-central1` (Iowa) - US alternative

### Machine Types

- `e2-standard-2` - Default (2 vCPU, 8GB RAM)
- `e2-standard-4` - For larger datasets

---

## 📊 Pipeline Execution Details

### Resource Management

```bash
# GCS Paths Used:
gs://bucket/temp/                    # Required by BigQuery for export operations
gs://bucket/staging/                 # Required by Dataflow for code staging
gs://bucket/output/results.txt       # Easy/Hard Task pipeline results
```

### File Output Behavior

- **Easy Task**: Creates `results.txt` with ride counts
- **Hard Task**: Creates `results.txt` with ride counts and distances
- **CSV format**:
  - Easy: `start_station_id,end_station_id,ride_count`
  - Hard: `start_station_id,end_station_id,ride_count,total_distance`

### Monitoring

- View jobs: [Google Cloud Console → Dataflow → Jobs](https://console.cloud.google.com/dataflow/jobs)
- Logs: Available in Cloud Logging with detailed transform-level logging
- Metrics: CPU, memory, throughput, and geopy calculation performance available in job details

---

## 🚀 Deployment Options

### Option 1: Enhanced Helper Scripts (Recommended)

```bash
# Easy Task with colored output and validation
./scripts/run_easy_task.sh your-project-id your-bucket-name

# Hard Task with distance calculations
./scripts/run_hard_task.sh your-project-id your-bucket-name

# With custom region
./scripts/run_easy_task.sh your-project-id your-bucket-name europe-west1
```

### Option 2: Direct Python Commands

```bash
# Easy Task
python main.py --task easy --project PROJECT --bucket BUCKET --region europe-west10

# Hard Task
python -m src.pipeline.hard_task_pipeline --project PROJECT --bucket BUCKET --region europe-west10
```

### Option 3: Custom Parameters

```bash
# Easy Task with custom settings
python main.py --task easy \
    --project your-project-id \
    --bucket your-bucket-name \
    --region us-central1 \
    --machine_type e2-standard-4 \
    --max_num_workers 10

# Hard Task with custom settings
python main.py --task hard \
    --project your-project-id \
    --bucket your-bucket-name \
    --region europe-west1 \
    --machine_type e2-standard-4 \
    --max_num_workers 20
```

---

## 📈 Performance & Scaling

### Expected Performance

- **Dataset Size**: ~20M bicycle hire records, ~800 station locations
- **Processing Time**:
  - Easy Task: 5-15 minutes on Dataflow
  - Hard Task: 10-25 minutes on Dataflow (includes geopy distance calculations)
- **Output Size**:
  - Easy Task: ~100KB (station pair combinations)
  - Hard Task: ~150KB (station pairs with distance data)

### Scaling Considerations

- **Auto-scaling**: Dataflow automatically scales workers based on data volume
- **Max Workers**: Default 100, configurable via `--max_num_workers`
- **Machine Type**: Larger types recommended for Hard Task due to geopy calculations
- **Geopy Performance**: Distance calculations are CPU-intensive but highly parallelizable

### Hard Task Optimizations

- **Side Input Pattern**: Station locations loaded as side input for efficient lookups
- **Haversine Formula**: `geopy.distance.geodesic` for accurate straight-line distances
- **Error Handling**: Graceful handling of missing station coordinates
- **Parallel Processing**: Each station pair calculation runs independently

---

## 🔍 Troubleshooting

### Common Issues

**Authentication Error:**

```bash
# Solution: Re-authenticate
gcloud auth application-default login
```

**BigQuery Permission Denied:**

```bash
# Solution: Ensure project has BigQuery API enabled
gcloud services enable bigquery.googleapis.com
```

**Dataflow Job Failed:**

```bash
# Check logs in Google Cloud Console
# Verify bucket exists and is accessible
gsutil ls gs://your-bucket-name/

# For Hard Task: Check geopy import issues
# Ensure geopy is properly packaged with the pipeline
```

**Import Errors:**

```bash
# Solution: Install in virtual environment
source .venv/bin/activate
pip install -e .

# For Hard Task geopy errors:
pip install geopy==2.4.1
```

**Hard Task Specific Issues:**

```bash
# Missing station coordinates error:
# Check if station data exists in BigQuery
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`bigquery-public-data.london_bicycles.cycle_stations\`"

# Geopy calculation errors:
# Verify latitude/longitude format in station data
# Check for NULL coordinates in station records
```

### Pipeline Debugging

1. **Check job status** in Google Cloud Console
2. **Review logs** for detailed error messages and transform-level debugging
3. **Verify data access** with BigQuery console
4. **Test locally** with `./scripts/test_easy_task.sh` and `./scripts/test_hard_task.sh`
5. **For Hard Task**: Validate geopy calculations with sample coordinates

---

## 🎓 Learning Resources

### Apache Beam Concepts Used

- **PCollection**: Distributed dataset representation
- **ParDo**: Parallel processing transform (replaces FlatMap for consistency)
- **GroupByKey**: Grouping data by key for aggregation
- **Side Inputs**: Efficient lookup patterns (used in Hard Task for station coordinates)
- **I/O Connectors**: BigQuery read, GCS write
- **Data Models**: Type-safe data structures with NamedTuple

### Google Cloud Services Used

- **Dataflow**: Managed Apache Beam execution with auto-scaling
- **BigQuery**: Data warehouse and SQL analytics for London bicycle data
- **Cloud Storage**: Object storage for pipeline artifacts and results

### Geographic Computing (Hard Task)

- **Geopy Library**: Geographic distance calculations
- **Haversine Formula**: Spherical distance calculation via `geodesic()`
- **Coordinate Systems**: Latitude/longitude decimal degrees
- **Straight-line Distance**: Great circle distance between two points on Earth

---

## ✅ Implementation Completed

### What's Been Implemented ✅

**Easy Task (Complete)**:

- ✅ Ride counting between station pairs
- ✅ Class-based DoFn transforms with consistent error handling
- ✅ Type-safe data models (`StationRideCount`)
- ✅ Centralized SQL queries in `queries.py`
- ✅ Comprehensive unit and integration tests
- ✅ Enhanced deployment scripts with colored output

**Hard Task (Complete)**:

- ✅ Distance calculation using geopy Haversine formula
- ✅ Dual BigQuery input processing (rides + station coordinates)
- ✅ Side input pattern for efficient station location lookups
- ✅ Class-based DoFn transforms matching Easy Task structure
- ✅ Type-safe data models (`StationDistanceResult`)
- ✅ Dedicated distance-specific SQL queries
- ✅ Comprehensive test coverage including geopy integration tests
- ✅ Separate pipeline orchestration in `hard_task_pipeline.py`

**Infrastructure & DevOps**:

- ✅ Enhanced deployment scripts with validation and colored output
- ✅ Dual test runners for both tasks
- ✅ Consistent project structure and patterns
- ✅ Production-ready error handling and logging

---

## ✅ Success Validation

### Easy Task Success Indicators:

- ✅ Dataflow job completes successfully
- ✅ Output file created at `gs://bucket/output/easy_task_results-*.txt`
- ✅ File contains CSV data with station pairs and ride counts
- ✅ All Easy Task tests pass locally (`./scripts/test_easy_task.sh`)

**Example Easy Task Output**:

```
1,2,1523
2,1,1498
1,3,892
3,1,845
...
```

### Hard Task Success Indicators:

- ✅ Dataflow job completes successfully
- ✅ Output file created at `gs://bucket/output/hard_task_results-*.txt`
- ✅ File contains CSV data with station pairs, ride counts, and total distances
- ✅ Distance calculations use geopy geodesic formula
- ✅ All Hard Task tests pass locally (`./scripts/test_hard_task.sh`)

**Example Hard Task Output**:

```
1,2,1523,2874.123456
2,1,1498,2824.567890
1,3,892,1456.789012
3,1,845,1381.234567
...
```

### Test Validation:

```bash
# Validate both implementations
./scripts/test_easy_task.sh
./scripts/test_hard_task.sh

# Expected: All tests pass with colored success output
# Easy Task: 4 tests passing
# Hard Task: 8 tests passing (including geopy integration)
```

This indicates successful implementation of both Easy and Hard tasks for London bicycle data analysis with comprehensive distance calculations! 🎉
