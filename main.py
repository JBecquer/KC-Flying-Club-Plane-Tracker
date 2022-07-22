"""
FCKC Plane Tracker

Author: Jordan Becquer
7/21/2022
"""

import sys
from math import radians, cos, sin, asin, sqrt


def flightaware_getter():
    """API call to grab data from flight aware and save to file."""
    pass


def calculate_stats():
    """ Calculate various stats related to the aircraft's history"""
    def dist_travelled():
        """Calculate the total distance travelled by the aircraft using lat/long data."""
        
        def lat_long_dist(lat1, lat2, lon1, lon2):
            """
            Calculate the distance between 2 sets of lat/long coordinates using the Haversine formula
            :param lat1: Latitude point 1
            :param lat2: Latitude point 2
            :param lon1: Longitude point 1
            :param lon2: Longitude point 2
            :return: Distance between the two points in miles
            :rtype: float
            """
            # The math module contains a function called radians which converts from degrees to radians.
            lon1 = radians(lon1)
            lon2 = radians(lon2)
            lat1 = radians(lat1)
            lat2 = radians(lat2)

            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2

            c = 2 * asin(sqrt(a))

            # Radius of earth in miles.
            r = 3958.8

            # calculate the result
            return c * r

        total_dist = 0
        
        #for i, coord in enumerate(coordinates[:-1:]):
            # total_dist += lat_long_dist(coord[i][0], coord[i][1], coord[i+1][0], coord[i+1][1]
        return total_dist

    def time_aloft():
        """Calculate the max time aloft by using the aircraft in-air data"""
        pass

    def airports_visited():
        """Determine the airports visited"""
        pass


def local_area_map():
    """Use the lat/long data to plot a composite map of the KC area"""
    pass


def conus_area_map():
    """Use the lat/long data to plot a composite map of the CONUS"""
    pass


def main():
    """Main entry point for the script."""
    pass


if __name__ == "__main__":
    sys.exit(main())
