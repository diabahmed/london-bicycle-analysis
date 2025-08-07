"""
Tests for the Hard Task Pipeline: Distance calculation transforms and pipeline.
"""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from geopy.distance import geodesic

from src.transforms.hard_task import (
    ExtractValidRidesForDistance,
    ExtractStationLocations,
    CountRidesByStationPairForDistance,
    CalculateDistance,
    FormatDistanceOutput,
    build_hard_task_pipeline
)
from src.models import StationDistanceResult


class TestExtractValidRidesForDistance(unittest.TestCase):
    """Test cases for ExtractValidRidesForDistance transform."""
    
    def test_valid_ride_extraction(self):
        """Test extraction of valid ride data for distance calculations."""
        with TestPipeline() as p:
            ride_data = [
                {'start_station_id': 1, 'end_station_id': 2},
                {'start_station_id': 2, 'end_station_id': 1},
                {'start_station_id': 1, 'end_station_id': 1},  # Same station - should be filtered
                {'start_station_id': 'invalid', 'end_station_id': 2},  # Invalid - should be filtered
            ]
            
            result = (
                p 
                | beam.Create(ride_data)
                | beam.ParDo(ExtractValidRidesForDistance())
            )
            
            expected = [
                (1, 2),
                (2, 1),
            ]
            
            assert_that(result, equal_to(expected))


class TestExtractStationLocations(unittest.TestCase):
    """Test cases for ExtractStationLocations transform."""
    
    def test_valid_station_extraction(self):
        """Test extraction of valid station location data."""
        with TestPipeline() as p:
            station_data = [
                {'id': 1, 'latitude': 51.5074, 'longitude': -0.1278},
                {'id': 2, 'latitude': 51.5155, 'longitude': -0.0922},
                {'id': 'invalid', 'latitude': 51.5074, 'longitude': -0.1278},  # Invalid - should be filtered
            ]
            
            result = (
                p 
                | beam.Create(station_data)
                | beam.ParDo(ExtractStationLocations())
            )
            
            expected = [
                (1, (51.5074, -0.1278)),
                (2, (51.5155, -0.0922)),
            ]
            
            assert_that(result, equal_to(expected))


class TestCountRidesByStationPairForDistance(unittest.TestCase):
    """Test cases for CountRidesByStationPairForDistance transform."""
    
    def test_count_rides_by_station_pair(self):
        """Test counting rides for station pairs."""
        with TestPipeline() as p:
            station_pair_data = [
                ((1, 2), [1, 1, 1]),  # 3 rides from station 1 to 2
                ((2, 1), [1, 1]),     # 2 rides from station 2 to 1
            ]
            
            result = (
                p 
                | beam.Create(station_pair_data)
                | beam.ParDo(CountRidesByStationPairForDistance())
            )
            
            expected = [
                ((1, 2), 3),
                ((2, 1), 2),
            ]
            
            assert_that(result, equal_to(expected))


class TestCalculateDistance:
    """Test cases for CalculateDistance transform."""
    
    def test_distance_calculation_with_models(self):
        """Test distance calculation using models."""
        with TestPipeline() as p:
            # Known coordinates for testing (London landmarks)
            station_locations = {
                1: (51.4994, -0.1245),  # Big Ben
                2: (51.5033, -0.1195),  # London Eye
            }
            
            station_pair_data = [
                ((1, 2), 3),  # 3 rides from station 1 to station 2
            ]
            
            result = (
                p 
                | beam.Create(station_pair_data)
                | beam.ParDo(CalculateDistance(), station_locations=station_locations)
            )
            
            # Manually calculate expected distance
            expected_distance = geodesic((51.4994, -0.1245), (51.5033, -0.1195)).kilometers
            total_distance = 3 * expected_distance
            
            expected_result = StationDistanceResult(
                start_station_id=1,
                end_station_id=2,
                ride_count=3,
                total_distance=total_distance
            )
            
            assert_that(result, equal_to([expected_result]))


class TestFormatDistanceOutput:
    """Test cases for FormatDistanceOutput transform."""
    
    def test_format_distance_output(self):
        """Test formatting of StationDistanceResult objects."""
        with TestPipeline() as p:
            # Create test data using the model
            test_result = StationDistanceResult(
                start_station_id=1,
                end_station_id=2,
                ride_count=3,
                total_distance=1.234567
            )
            
            result = (
                p 
                | beam.Create([test_result])
                | beam.ParDo(FormatDistanceOutput())
            )
            
            expected_output = "1,2,3,1.234567"
            assert_that(result, equal_to([expected_output]))


class TestHardTaskPipeline(unittest.TestCase):
    """Test cases for the complete hard task pipeline."""
    
    def test_build_hard_task_pipeline(self):
        """Test the complete hard task pipeline logic."""
        with TestPipeline() as p:
            # Sample station data
            station_data = [
                {'id': 1, 'latitude': 51.4994, 'longitude': -0.1245},  # Big Ben
                {'id': 2, 'latitude': 51.5033, 'longitude': -0.1195},  # London Eye
            ]
            
            # Sample ride data
            ride_data = [
                {'start_station_id': 1, 'end_station_id': 2},
                {'start_station_id': 1, 'end_station_id': 2},  # 2 rides 1->2
                {'start_station_id': 2, 'end_station_id': 1},  # 1 ride 2->1
            ]
            
            # Create PCollections
            cycle_hire_data = p | "Create Ride Data" >> beam.Create(ride_data)
            cycle_stations_data = p | "Create Station Data" >> beam.Create(station_data)
            
            # Process through pipeline
            results = build_hard_task_pipeline(cycle_hire_data, cycle_stations_data)
            
            # Manually calculate expected distances
            dist_1_to_2 = geodesic((51.4994, -0.1245), (51.5033, -0.1195)).kilometers
            dist_2_to_1 = dist_1_to_2  # Same distance
            
            expected = [
                f"1,2,2,{2 * dist_1_to_2:.6f}",
                f"2,1,1,{1 * dist_2_to_1:.6f}",
            ]
            
            assert_that(results, equal_to(expected))


class TestGeopyIntegration(unittest.TestCase):
    """Test integration with geopy library for distance calculations."""
    
    def test_geopy_distance_accuracy(self):
        """Test that geopy calculations match expected values for known distances."""
        # Test known distance: Big Ben to London Eye approximately 0.57 km
        big_ben = (51.4994, -0.1245)
        london_eye = (51.5033, -0.1195)
        
        distance = geodesic(big_ben, london_eye).kilometers
        
        # Should be approximately 0.57 km (within reasonable tolerance)
        assert 0.4 <= distance <= 0.8, f"Distance {distance} km not within expected range"
    
    def test_geopy_same_location(self):
        """Test that geopy returns 0 for same location."""
        location = (51.5074, -0.1278)
        distance = geodesic(location, location).kilometers
        
        assert distance == 0.0


if __name__ == '__main__':
    unittest.main()