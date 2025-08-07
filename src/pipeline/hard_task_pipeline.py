"""Main pipeline for the London Bicycles hard task data analysis."""

import argparse
import logging
from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.filesystem import CompressionTypes

from config import Config
from src.queries import BigQueryQueries
from src.transforms.hard_task import build_hard_task_pipeline


def run_hard_task_pipeline(pipeline_args: Optional[list] = None) -> None:
    """
    Run the hard task pipeline to calculate total distance covered by station pairs.
    
    Args:
        pipeline_args: Command line arguments for the pipeline
    """
    # Parse pipeline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        required=True,
        help='GCP Project ID'
    )
    parser.add_argument(
        '--region',
        default='europe-west10',
        help='GCP Region for Dataflow jobs'
    )
    parser.add_argument(
        '--bucket',
        required=True,
        help='GCS bucket name for output and staging'
    )
    parser.add_argument(
        '--runner',
        default='DataflowRunner',
        help='Pipeline runner (DataflowRunner for GCP)'
    )
    
    known_args, beam_args = parser.parse_known_args(pipeline_args)
    
    # Set up GCS paths (auto-generated)
    temp_location = f"gs://{known_args.bucket}/temp"          # Required by BigQuery for temporary data export
    staging_location = f"gs://{known_args.bucket}/staging"    # Required by Dataflow for code staging
    output_path = f"gs://{known_args.bucket}/output/results"  # Our pipeline output location
    
    # Configure Dataflow pipeline options
    dataflow_options = Config.get_dataflow_options(
        project_id=known_args.project,
        region=known_args.region,
        temp_location=temp_location,
        staging_location=staging_location
    )
    
    # Use the runner from command line arguments
    dataflow_options['runner'] = known_args.runner
    pipeline_options = PipelineOptions(flags=beam_args, **dataflow_options)
    
    # Set up logging
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting Hard Task Pipeline")
    
    # Run the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read cycle hire data from BigQuery
        cycle_hire_data = (
            pipeline
            | "Read Cycle Hire Data" >> ReadFromBigQuery(
                query=BigQueryQueries.get_cycle_hire_query(),
                use_standard_sql=True
            )
        )
        
        # Read cycle stations data from BigQuery
        cycle_stations_data = (
            pipeline
            | "Read Cycle Stations Data" >> ReadFromBigQuery(
                query=BigQueryQueries.get_cycle_stations_query(),
                use_standard_sql=True
            )
        )
        
        # Process data through the hard task pipeline
        results = build_hard_task_pipeline(cycle_hire_data, cycle_stations_data)
        
        # Write results to GCS as hard_task_results.txt (will overwrite on each run)
        (
            results
            | "Write to GCS" >> beam.io.WriteToText(
                output_path,
                file_name_suffix='.txt',
                compression_type=CompressionTypes.UNCOMPRESSED,
                num_shards=1,  # Single output file
                shard_name_template=''  # No shard suffix - creates clean hard_task_results.txt
            )
        )
    
    logging.info(f"Hard Task Pipeline completed. Results written to {output_path}.txt")


if __name__ == '__main__':
    run_hard_task_pipeline()
