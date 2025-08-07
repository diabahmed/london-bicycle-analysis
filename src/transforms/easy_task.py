"""Apache Beam transforms for the Easy Task: calculating ride counts."""

import apache_beam as beam
from apache_beam.pvalue import PCollection
from typing import Tuple, Iterator
import logging

from src.models import StationRideCount


class ExtractValidRides(beam.DoFn):
    """Extract valid rides with both start and end station IDs."""
    
    def process(self, element) -> Iterator[Tuple[int, int]]:
        """
        Process a cycle hire record and extract valid station pairs.
        
        Args:
            element: Dictionary containing cycle hire data from BigQuery
            
        Yields:
            Tuple of (start_station_id, end_station_id) for valid rides
        """
        try:
            start_station_id = element.get('start_station_id')
            end_station_id = element.get('end_station_id')
            
            # Only process rides with valid start and end stations
            if (start_station_id is not None and 
                end_station_id is not None and
                isinstance(start_station_id, int) and 
                isinstance(end_station_id, int)):
                
                yield (start_station_id, end_station_id)
                
        except Exception as e:
            logging.warning(f"Error processing ride record: {e}")
            # Skip invalid records


class CountRidesByStationPair(beam.DoFn):
    """Count rides for each station pair."""
    
    def process(self, element):
        """
        Process grouped station pairs and count rides.
        
        Args:
            element: Tuple containing ((start_station_id, end_station_id), iterable_of_ones)
            
        Yields:
            StationRideCount object with ride count for the station pair
        """
        try:
            (start_station_id, end_station_id), ones = element
            ride_count = sum(ones)
            
            yield StationRideCount(
                start_station_id=start_station_id,
                end_station_id=end_station_id,
                ride_count=ride_count
            )
            
        except Exception as e:
            logging.error(f"Error counting rides for station pair: {e}")


class FormatRideCountOutput(beam.DoFn):
    """Format ride count results for output."""
    
    def process(self, element) -> Iterator[str]:
        """
        Format StationRideCount for output file.
        
        Args:
            element: StationRideCount object
            
        Yields:
            Formatted string: "start_station_id,end_station_id,ride_count"
        """
        try:
            yield f"{element.start_station_id},{element.end_station_id},{element.ride_count}"
        except Exception as e:
            logging.error(f"Error formatting ride count output: {e}")


def build_easy_task_pipeline(cycle_hire_data: PCollection) -> PCollection[str]:
    """
    Build the pipeline for the easy task: counting rides by station pairs.
    
    Args:
        cycle_hire_data: PCollection of cycle hire records from BigQuery
        
    Returns:
        PCollection of formatted output strings
    """
    return (
        cycle_hire_data
        | "Extract Valid Rides" >> beam.ParDo(ExtractValidRides())
        | "Add Count Value" >> beam.Map(lambda x: (x, 1))
        | "Group By Station Pair" >> beam.GroupByKey()
        | "Count Rides" >> beam.ParDo(CountRidesByStationPair())
        | "Format Output" >> beam.ParDo(FormatRideCountOutput())
    )
