"""Main entry point for the London Bicycles data pipeline."""

import sys
import argparse
import logging

from src.pipeline.easy_task_pipeline import run_easy_task_pipeline
from src.pipeline.hard_task_pipeline import run_hard_task_pipeline


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description='London Bicycles Data Analysis Pipeline')
    parser.add_argument(
        '--task',
        choices=['easy', 'hard'],
        required=True,
        help='Which task to run: easy (ride counts) or hard (distance calculations)'
    )
    
    # Parse only the task argument first
    known_args, remaining_args = parser.parse_known_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if known_args.task == 'easy':
        logging.info("Running Easy Task: Calculate ride counts by station pairs")
        run_easy_task_pipeline(remaining_args)
    elif known_args.task == 'hard':
        logging.info("Running Hard Task: Calculate distances by station pairs")
        run_hard_task_pipeline(remaining_args)
    else:
        logging.error(f"Unknown task: {known_args.task}")
        sys.exit(1)


if __name__ == '__main__':
    main()
