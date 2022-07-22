"""
FCKC Plane Tracker

Author: Jordan Becquer
7/21/2022
"""

import sys


def flightaware_getter():
    """API call to grab data from flight aware and save to file."""
    pass


def calculate_stats():
    """ Calculate various stats related to the aircraft's history"""
    def dist_travelled():
        """Calculate the total distance travelled by the aircraft using lat/long data."""
        pass

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