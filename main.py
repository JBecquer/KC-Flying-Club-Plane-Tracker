"""
FCKC Plane Tracker

Author: Jordan Becquer
7/21/2022
"""

import sys
from math import radians, cos, sin, asin, sqrt
import mysql.connector
from getpass import getpass


def mysql_connect(aircraft):
    """
    Connect to MySQL server, and grab database using aircraft ID
    :param aircraft: Tail number of the aircraft
    :type aircraft: str
    :return: mysql.connector.connect() is pass, Exception if fail
    """
    # TODO FIGURE OUT A WAY TO VERIFY IF A CONNECTION HAS ALREADY BEEN ESTABLISHED, SO IT IS NOT NEEDED TO ENTER THE
    #  PW MULTIPLE TIMES
    try:
        # Init connection to MySQL database
        db = mysql.connector.connect(
            host="localhost",
            user="JBecquer",
            passwd=getpass("Enter MySQL password: "),
            database=aircraft
        )
        print(f" Database connection to {aircraft} successful.")
        return db
    except Exception as x:
        print(f" Database connection failed!")
        sys.exit(x)


def flightaware_getter():
    """Web scraping to grab data from flight aware and save to file."""
    pass


def db_data_saver(fleet):
    """Export the web scrapped data into MySQL"""

    # Establish connection with MySQL
    db = mysql_connect(fleet[0])

    # TODO CLEAN-UP MYSQL SETUP
    # initialize the MySQL cursor
    mycursor = db.cursor()

    # Create a database
    # mycursor.execute("CREATE DATABASE [IF NOT EXIST] N81673")

    # Create a table
    # mycursor.execute("CREATE TABLE Flight (Date DATE, Time TIME, Latitude FLOAT, Longitude FLOAT, Knots TINYINT(3),
    # Altitude MEDIUMINT(5))")

    # Delete a table
    # mycursor.execute("DROP TABLE Flight")

    dummy_data = [["2022-7-22", "01:19:23", 42.1152, -92.9207, 88, 1000],
                  ["2022-7-22", "01:19:39", 42.1193, -92.9279, 89, 1200],
                  ["2022-7-22", "01:19:55", 42.1207, -92.9358, 89, 1400]
                  ]

    for step in dummy_data:
        mycursor.execute("INSERT INTO Flight (Date, Time, Latitude, Longitude, Knots, Altitude) "
                         "VALUES (%s,%s,%s,%s,%s,%s)",
                         (step[0], step[1], step[2], step[3], step[4], step[5]))
        db.commit()

    # Print all from table Flight
    mycursor.execute("SELECT * FROM Flight")
    for x in mycursor:
        print(x)

    db.close()

def db_data_getter(fleet):
    """Import the data from MySQL and convert into pandas """

    # Establish connection with MySQL
    db = mysql_connect(fleet[0])

    mycursor = db.cursor()

    db.cose()
    pass


def calculate_stats(fleet):
    """ Calculate various stats related to the aircraft's history"""

    # Establish connection with MySQL:
    db = mysql_connect(fleet[0])

    def dist_travelled():
        """Calculate the total distance travelled by the aircraft using lat/long data.

        :return: Total distance travelled in miles
        :rtype: float(2)
        """

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
            a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2

            c = 2 * asin(sqrt(a))

            # Radius of earth in miles.
            r = 3958.8

            # calculate the result
            return c * r

        # initialize the MySQL cursor
        mycursor = db.cursor()

        # grab the latitude and longitude data from MySQL
        mycursor.execute("SELECT Latitude, Longitude FROM Flight")

        latitude = []
        longitude = []
        for x in mycursor:
            latitude.append(x[0])
            longitude.append(x[1])

        total_dist = 0
        # TODO CREATE AN EXCEPTION FOR WHEN LEN(LAT) != LEN(LONG)
        for x in range(len(latitude[:-1:])):
            total_dist += lat_long_dist(latitude[x], latitude[x + 1], longitude[x], longitude[x + 1])
        print(f" The total distance travelled was {round(total_dist, 2)} Miles")
        return total_dist

    def time_aloft():
        """Calculate the max time aloft by using the aircraft in-air data"""
        pass

    def airports_visited():
        """Determine the airports visited"""
        pass

    # calculate_stats function calls
    dist_travelled()
    time_aloft()
    airports_visited()

    db.close()

def local_area_map():
    """Use the lat/long data to plot a composite map of the KC area"""
    pass


def conus_area_map():
    """Use the lat/long data to plot a composite map of the CONUS"""
    pass


def main():
    """Main entry point for the script."""

    fleet = [
        "N81673",  # Archer
        "N3892Q",  # C172
        "N20389",  # C172
        "N182WK",  # C182
        "N58843",  # C182
        "N82145",  # Saratoga
        "N4803P"   # Debonair
    ]

    # db_data_saver(fleet)
    calculate_stats(fleet)
    pass


if __name__ == "__main__":
    sys.exit(main())
