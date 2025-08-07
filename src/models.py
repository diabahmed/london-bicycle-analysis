"""Data models for the London Bicycles pipeline."""

from typing import NamedTuple


class StationRideCount(NamedTuple):
    """Data model for station ride count results."""
    start_station_id: int
    end_station_id: int
    ride_count: int


class StationDistanceResult(NamedTuple):
    """Data model for station distance calculation results."""
    start_station_id: int
    end_station_id: int
    ride_count: int
    total_distance: float
