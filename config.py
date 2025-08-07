"""Configuration module for the London Bicycles Data Pipeline."""

from typing import Dict, Any


class Config:
    """Configuration class containing all pipeline settings."""
    
    # BigQuery dataset and table configurations
    BIGQUERY_PROJECT = "bigquery-public-data"
    BIGQUERY_DATASET = "london_bicycles"
    CYCLE_HIRE_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cycle_hire"
    CYCLE_STATIONS_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.cycle_stations"
    
    # GCS configuration variables
    # those will be set through the command line
    GOOGLE_CLOUD_PROJECT = None
    BUCKET_NAME = None

    # Pipeline configuration
    PIPELINE_NAME = "london-bicycles-analysis"
    PIPELINE_VERSION = "1.0.0"
    
    # Dataflow configuration defaults
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
    
    @classmethod
    def get_dataflow_options(cls, project_id: str, region: str, 
                           temp_location: str, staging_location: str,
                           additional_options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get complete Dataflow pipeline options."""
        options = cls.DEFAULT_DATAFLOW_OPTIONS.copy()
        
        # Format placeholders in docker registry URL
        if 'docker_registry_push_url' in options:
            options['docker_registry_push_url'] = options['docker_registry_push_url'].format(
                project=project_id, region=region
            )
        
        options.update({
            'project': project_id,
            'region': region,
            'temp_location': temp_location,
            'staging_location': staging_location,
            'job_name': f"{cls.PIPELINE_NAME}-{cls.PIPELINE_VERSION}".replace('.', '-').lower(),
        })
        
        if additional_options:
            options.update(additional_options)
            
        return options
