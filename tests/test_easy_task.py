"""Unit tests for the easy task transforms."""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to

from src.transforms.easy_task import (
    ExtractValidRides, 
    CountRidesByStationPair, 
    FormatRideCountOutput,
    build_easy_task_pipeline
)
from src.models import StationRideCount


class TestEasyTaskTransforms(unittest.TestCase):
    """Test cases for easy task transforms."""
    
    def test_extract_valid_rides(self):
        """Test ExtractValidRides transform."""
        transform = ExtractValidRides()
        
        # Test valid ride
        valid_ride = {
            'start_station_id': 1,
            'end_station_id': 2,
            'rental_id': 123
        }
        result = list(transform.process(valid_ride))
        self.assertEqual(result, [(1, 2)])
        
        # Test invalid ride (missing end station)
        invalid_ride = {
            'start_station_id': 1,
            'end_station_id': None,
            'rental_id': 123
        }
        result = list(transform.process(invalid_ride))
        self.assertEqual(result, [])
        
        # Test invalid ride (missing start station)
        invalid_ride2 = {
            'start_station_id': None,
            'end_station_id': 2,
            'rental_id': 123
        }
        result = list(transform.process(invalid_ride2))
        self.assertEqual(result, [])
    
    def test_count_rides_by_station_pair(self):
        """Test CountRidesByStationPair transform."""
        transform = CountRidesByStationPair()
        
        # Test counting rides
        element = ((1, 2), [1, 1, 1])  # 3 rides from station 1 to 2
        result = list(transform.process(element))
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].start_station_id, 1)
        self.assertEqual(result[0].end_station_id, 2)
        self.assertEqual(result[0].ride_count, 3)
    
    def test_format_ride_count_output(self):
        """Test FormatRideCountOutput transform."""
        transform = FormatRideCountOutput()
        
        station_ride_count = StationRideCount(
            start_station_id=1,
            end_station_id=2,
            ride_count=5
        )
        
        result = list(transform.process(station_ride_count))
        self.assertEqual(result, ["1,2,5"])


class TestEasyTaskPipeline(unittest.TestCase):
    """Integration tests for the easy task pipeline."""
    
    def test_build_easy_task_pipeline(self):
        """Test the complete easy task pipeline."""
        with BeamTestPipeline() as p:
            # Sample input data
            input_data = [
                {'start_station_id': 1, 'end_station_id': 2},
                {'start_station_id': 1, 'end_station_id': 2},
                {'start_station_id': 2, 'end_station_id': 1},
                {'start_station_id': 1, 'end_station_id': 3},
                {'start_station_id': None, 'end_station_id': 2},  # Invalid
            ]
            
            input_pcollection = p | beam.Create(input_data)
            result = build_easy_task_pipeline(input_pcollection)
            
            # Expected output (order might vary due to parallel processing)
            expected = [
                "1,2,2",  # 2 rides from station 1 to 2
                "2,1,1",  # 1 ride from station 2 to 1
                "1,3,1",  # 1 ride from station 1 to 3
            ]
            
            assert_that(result, equal_to(expected))


if __name__ == '__main__':
    unittest.main()
