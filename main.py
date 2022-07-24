"""
FCKC Plane Tracker

Author: Jordan Becquer
7/21/2022
"""

import sys
from math import radians, cos, sin, asin, sqrt
import mysql.connector
from getpass import getpass
import requests
from bs4 import BeautifulSoup
import pandas as pd


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
    """
    Web scraping to grab data from flight aware and save to file.
    :return: Panda dataframe containing list[time, lat, long, kts, altitude]
    """
    def convert24(str1):
        """
        Convert from 12-hour to 24-hour format
        :param str1: 12-hour time string with AM/PM suffix
        :return: 24-hour format (HH:MM:SS)
        :rtype: str
        """
        # Checking if last two elements of time
        # is AM and first two elements are 12
        if str1[-2:] == "AM" and str1[:2] == "12":
            return "00" + str1[2:-2]

        # remove the AM
        elif str1[-2:] == "AM":
            return str1[:-2]

        # Checking if last two elements of time
        # is PM and first two elements are 12
        elif str1[-2:] == "PM" and str1[:2] == "12":
            return str1[:-2]
        else:
            # add 12 to hours and remove PM
            return str(int(str1[:2]) + 12) + str1[2:8]

    # Make a GET request to flightaware
    # TODO CREATE A WAY TO GET THE URL FOR NEW FLIGHT LEGS
    # TODO TAKE IN AIRCRAFT ID, CHECK PAGE, CHECK IF ANY NEW FLIGHTS. IF YES, CONTINUE, ELSE EXIT
    url = "https://flightaware.com/live/flight/N81673/history/20220722/2102Z/MO3/KOJC/tracklog"
    r = requests.get(url)
    # Check the status code
    if r.status_code != 200:
        print(f" Failed to connect to FlightAware!")
        print(f" status code: {r.status_code}")
        sys.exit()

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")
    # ------------------------------------------------------------------------------------------------------------------
    #   Extract flight leg information
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    #   Extract table data
    # ------------------------------------------------------------------------------------------------------------------
    # Look for table "prettyTable"
    try:
        table = soup.find("table", class_="prettyTable fullWidth")
    except Exception as e:
        print(f" Error finding table on FlightAware!")
        sys.exit(e)

    # Defining of the dataframe
    df = pd.DataFrame(columns=["Time", "Latitude", "Longitude", "Kts", "Altitude"])

    # Scrape data
    rows = table.find_all("tr")
    # reject the first two rows, these are headers
    for row in rows[2::]:
        """
        "span"
        class_ = show-for-medium-up
        [0] = Time (EDT)
        [1] = Latitude
        [2] = Longitude
        [3] = altitude
        [4] = Altitude delta
        ~~~~~~~~~~~~~~~~~~~
        "td"
        class_ = show-for-medium-up-table
        [0] = kts
        [1] = Altitude delta
        """
        builder = []
        if len(row) == 21:
            columns = row.find_all('span', class_="show-for-medium-up")
            if len(columns) == 5:
                time = columns[0].text.strip()
                time = time[3::].strip()  # remove the leading three letter weekday
                time = convert24(time)  # convert from 12-hour to 24-hour
                latitude = columns[1].text.strip()
                longitude = columns[2].text.strip()
                altitude = columns[3].text.strip()
                altitude = altitude.replace(",", "")  # remove the comma to allow int() conversion
            else:
                continue
            kts_columns = row.find_all("td", class_="show-for-medium-up-table")
            if len(kts_columns) == 2:
                kts = kts_columns[0].text.strip()
            else:
                continue
            builder = [time, float(latitude), float(longitude), int(kts), int(altitude)]
        # Sometimes an empty list is generated due to scraping, reject these.
        if len(builder) == 5:
            df.loc[len(df)] = builder

    return df


def db_data_saver(fleet):
    """Export the web scrapped data into MySQL"""

    # Get pandas dataframe
    df = flightaware_getter()

    print(df)

    # Establish connection with MySQL
    # db = mysql_connect(fleet[0])
    #
    # # TODO CLEAN-UP MYSQL SETUP
    # # initialize the MySQL cursor
    # mycursor = db.cursor()
    #
    # # Create a database
    # # mycursor.execute("CREATE DATABASE [IF NOT EXIST] N81673")
    #
    # # Create a table
    # # mycursor.execute("CREATE TABLE Flight (Date DATE, Time TIME, Latitude FLOAT, Longitude FLOAT, Knots TINYINT(3),
    # # Altitude MEDIUMINT(5))")
    #
    # # Delete a table
    # # mycursor.execute("DROP TABLE Flight")
    #
    # dummy_data = [["2022-7-22", "01:19:23", 42.1152, -92.9207, 88, 1000],
    #               ["2022-7-22", "01:19:39", 42.1193, -92.9279, 89, 1200],
    #               ["2022-7-22", "01:19:55", 42.1207, -92.9358, 89, 1400]
    #               ]
    #
    # for step in dummy_data:
    #     mycursor.execute("INSERT INTO Flight (Date, Time, Latitude, Longitude, Knots, Altitude) "
    #                      "VALUES (%s,%s,%s,%s,%s,%s)",
    #                      (step[0], step[1], step[2], step[3], step[4], step[5]))
    #     db.commit()
    #
    # # Print all from table Flight
    # mycursor.execute("SELECT * FROM Flight")
    # for x in mycursor:
    #     print(x)
    #
    # db.close()


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
        "N4803P"  # Debonair
    ]

    db_data_saver(fleet)
    # calculate_stats(fleet)
    # flightaware_getter()
    pass


if __name__ == "__main__":
    sys.exit(main())
