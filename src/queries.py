"""BigQuery SQL queries for data extraction."""


class BigQueryQueries:
    """Container for BigQuery SQL queries."""
    
    @staticmethod
    def get_cycle_hire_query() -> str:
        """
        Get the SQL query to extract cycle hire data.
        """
        return """
        SELECT
            start_station_id,
            end_station_id
        FROM `bigquery-public-data.london_bicycles.cycle_hire`
        """
    
    @staticmethod
    def get_cycle_stations_query() -> str:
        """
        Get the SQL query to extract cycle station data.
        """
        return """
        SELECT
            id,
            latitude,
            longitude
        FROM `bigquery-public-data.london_bicycles.cycle_stations`
        """
