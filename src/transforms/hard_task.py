"""Apache Beam transforms for the Hard Task: calculating distances and ride counts."""

import apache_beam as beam
from apache_beam.pvalue import PCollection, AsDict
from geopy.distance import geodesic
from typing import Tuple, Iterator
import logging

from src.models import StationDistanceResult


class ExtractValidRidesForDistance(beam.DoFn):
    """Extract valid rides with both start and end station IDs for distance calculations."""
    
    def process(self, element) -> Iterator[Tuple[int, int]]:
        """
        Process a cycle hire record and extract valid station pairs for distance calculation.
        
        Args:
            element: Dictionary containing cycle hire data from BigQuery
            
        Yields:
            Tuple of (start_station_id, end_station_id) for valid rides
        """
        try:
            start_station_id = element.get('start_station_id')
            end_station_id = element.get('end_station_id')
            
            # Only process rides with valid start and end stations that are different
            if (start_station_id is not None and 
                end_station_id is not None and
                isinstance(start_station_id, int) and 
                isinstance(end_station_id, int) and
                start_station_id != end_station_id):
                
                yield (start_station_id, end_station_id)
                
        except Exception as e:
            logging.warning(f"Error processing ride record for distance: {e}")
            # Skip invalid records


class ExtractStationLocations(beam.DoFn):
    """Extract station location information."""
    
    def process(self, element) -> Iterator[Tuple[int, Tuple[float, float]]]:
        """
        Process a station record and extract location data.
        
        Args:
            element: Dictionary containing station data from BigQuery
            
        Yields:
            Tuple of (station_id, (latitude, longitude))
        """
        try:
            station_id = element.get('id')
            latitude = element.get('latitude')
            longitude = element.get('longitude')
            
            # Only process stations with valid location data
            if (station_id is not None and 
                latitude is not None and 
                longitude is not None):
                
                station_id = int(station_id)
                latitude = float(latitude)
                longitude = float(longitude)
                
                yield (station_id, (latitude, longitude))
                
        except Exception as e:
            logging.warning(f"Error processing station record: {e}")
            # Skip invalid records


class CountRidesByStationPairForDistance(beam.DoFn):
    """Count rides for each station pair for distance calculations."""
    
    def process(self, element):
        """
        Process grouped station pairs and count rides.
        
        Args:
            element: Tuple containing ((start_station_id, end_station_id), iterable_of_ones)
            
        Yields:
            Tuple of ((start_station_id, end_station_id), ride_count) - keeping tuple for distance calculation
        """
        try:
            (start_station_id, end_station_id), ones = element
            ride_count = sum(ones)
            
            # Keep as tuple for the distance calculation step
            yield ((start_station_id, end_station_id), ride_count)
            
        except Exception as e:
            logging.error(f"Error counting rides for station pair distance: {e}")


class CalculateDistance(beam.DoFn):
    """Calculate distance between stations and create StationDistanceResult objects."""
    
    def process(self, element, station_locations) -> Iterator[StationDistanceResult]:
        """
        Calculate distance and create StationDistanceResult for station pairs.
        
        Args:
            element: Tuple of ((start_station_id, end_station_id), ride_count)
            station_locations: Side input dictionary mapping station_id -> (lat, lon)
            
        Yields:
            StationDistanceResult object with calculated distance
        """
        try:
            (start_station_id, end_station_id), ride_count = element
            
            # Get station locations
            start_location = station_locations.get(start_station_id)
            end_location = station_locations.get(end_station_id)
            
            if start_location is None:
                logging.debug(f"Missing location for start station {start_station_id}")
                return
                
            if end_location is None:
                logging.debug(f"Missing location for end station {end_station_id}")
                return
            
            # Calculate straight-line distance using Haversine formula
            distance_km = geodesic(start_location, end_location).kilometers
            
            # Calculate total distance covered (ride_count * distance_between_stations)
            total_distance = ride_count * distance_km
            
            # Create StationDistanceResult object
            yield StationDistanceResult(
                start_station_id=start_station_id,
                end_station_id=end_station_id,
                ride_count=ride_count,
                total_distance=total_distance
            )
            
        except Exception as e:
            logging.error(f"Error calculating distance for stations {start_station_id}-{end_station_id}: {e}")


class FormatDistanceOutput(beam.DoFn):
    """Format StationDistanceResult for output."""
    
    def process(self, element: StationDistanceResult) -> Iterator[str]:
        """
        Format StationDistanceResult for output file.
        
        Args:
            element: StationDistanceResult object
            
        Yields:
            Formatted string: "start_station_id,end_station_id,ride_count,total_distance"
        """
        try:
            yield f"{element.start_station_id},{element.end_station_id},{element.ride_count},{element.total_distance:.6f}"
        except Exception as e:
            logging.error(f"Error formatting distance output: {e}")


def build_hard_task_pipeline(cycle_hire_data: PCollection, cycle_stations_data: PCollection) -> PCollection[str]:
    """
    Build the pipeline for the hard task: calculating distances and ride counts by station pairs.
    
    Args:
        cycle_hire_data: PCollection of cycle hire records from BigQuery
        cycle_stations_data: PCollection of cycle station records from BigQuery
        
    Returns:
        PCollection of formatted output strings
    """
    # Extract station locations as a side input
    station_locations = (
        cycle_stations_data
        | "Extract Station Locations" >> beam.ParDo(ExtractStationLocations())
    )
    
    # Process ride data and calculate distances
    return (
        cycle_hire_data
        | "Extract Valid Rides for Distance" >> beam.ParDo(ExtractValidRidesForDistance())
        | "Add Count Value" >> beam.Map(lambda x: (x, 1))
        | "Group By Station Pair" >> beam.GroupByKey()
        | "Count Rides for Distance" >> beam.ParDo(CountRidesByStationPairForDistance())
        | "Calculate Distances" >> beam.ParDo(
            CalculateDistance(),
            station_locations=AsDict(station_locations)
        )
        | "Format Distance Output" >> beam.ParDo(FormatDistanceOutput())
    )
