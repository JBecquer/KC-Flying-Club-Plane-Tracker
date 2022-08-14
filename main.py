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
from sqlalchemy import create_engine
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
import logging
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from time import sleep
from datetime import datetime


# create logger (copied from https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial)
# logging.basicConfig(filename="logname.txt",
#                     filemode="w+",
#                     format="%(levelname)s - %(message)s",
#                     level=logging.DEBUG)

logger = logging.getLogger('Main')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)


def mysql_connect(aircraft):
    """
    Connect to MySQL server, and grab database using aircraft ID
    :param aircraft: Tail number of the aircraft
    :type aircraft: str
    :return: mysql.connector.connect() is pass, Exception if fail
    """
    try:
        # Init connection to MySQL database
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd=pw,
            database=aircraft
        )
        logger.info(f" Database connection to {aircraft} successful.")
        return db
    except Exception as e:
        logger.critical(f" {aircraft} database connection failed! (mysql_connect)")
        sys.exit(e)

        
def between_parentheses(s):
    """
    Take in a string and return what is in-between the parentheses.  # TODO CREATE ERROR CONDITIONS
    :param s: string containing closed parantheses.
    :return: string contained between the parantheses.
    """
    res = []
    # Extracting from: Flight Track Log ✈ N81673 22-Jul-2022 (MO3-KOJC) - FlightAware
    # leg information is between the parenthesis, below code extracts and saves as separate objects
    for i in range(len(s)):
        if s[i] == "(":
            i = i + 1
            for j in range(len(s) - i):
                if s[i + j] == ")":
                    return "".join(res)
                else:
                    res.append(s[i + j])


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


def convert_date(s):
    """
    Take a date format "DD-MMM-YYYY" where MMM is a 3-digit month code. Convert to YYYY-MM-DD for MySQL DATE format
    ex: 17-JUL-2022 converts to 2022-07-17
    :param s: input date string DD-MMM-YYYY "17-JUL-2022"
    :return: output date string YYYY-MM-DD "2022-07-17"
    """
    if len(s) != 11:
        logger.critical(f" The input string {s} is not the correct length! ({len(s)} != 11)")
        sys.exit()

    if s[3:6].lower() == "jan":
        return s[7:] + "-01-" + s[0:2]
    if s[3:6].lower() == "feb":
        return s[7:] + "-02-" + s[0:2]
    if s[3:6].lower() == "mar":
        return s[7:] + "-03-" + s[0:2]
    if s[3:6].lower() == "apr":
        return s[7:] + "-04-" + s[0:2]
    if s[3:6].lower() == "may":
        return s[7:] + "-05-" + s[0:2]
    if s[3:6].lower() == "jun":
        return s[7:] + "-06-" + s[0:2]
    if s[3:6].lower() == "jul":
        return s[7:] + "-07-" + s[0:2]
    if s[3:6].lower() == "aug":
        return s[7:] + "-08-" + s[0:2]
    if s[3:6].lower() == "sep":
        return s[7:] + "-09-" + s[0:2]
    if s[3:6].lower() == "oct":
        return s[7:] + "-10-" + s[0:2]
    if s[3:6].lower() == "nov":
        return s[7:] + "-11-" + s[0:2]
    if s[3:6].lower() == "dec":
        return s[7:] + "-12-" + s[0:2]
    else:
        sys.exit(" Invalid date code! (convert_date)")


def flightaware_history(aircraft):
    """
    Grab the aircraft history from flight aware and return pandas dataframe containing history data.
    :param aircraft: aircraft ID. ex: N182WK
    :type aircraft: str
    :return: pandas df = [date, route, dept_time, URL]
    """
    headers = {
        'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/103.0.0.0 Safari/537.36',
        'Accept-Language': "en-US,en;q=0.9",
        'Referer': "https://google.com",
        "DNT": "1"
    }
    # Make a GET request to flightaware
    url = f"https://flightaware.com/live/flight/{aircraft}/history/80"
    logger.info(f" Getting plane history from: {url}")
    r = requests.get(url, headers=headers, timeout=5)
    # Check the status code
    if r.status_code != 200:
        logger.critical(f" Failed to connect to FlightAware! URL: {url}")
        logger.critical(f" Status code: {r.status_code}")
        sys.exit()

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")

    try:
        # ------------------------------------------------------------------------------------------------------------------
        #   Extract table data
        # ------------------------------------------------------------------------------------------------------------------
        # Look for table "prettyTable fullWidth tablesaw tablesaw-stack"
        try:
            table = soup.find("table", class_="prettyTable fullWidth tablesaw tablesaw-stack")
        except Exception as e:
            logger.critical(f" Error finding aircraft history table on FlightAware! (flightaware_history)")
            sys.exit(e)

        # Define of the dataframe
        df = pd.DataFrame(columns=["date", "route", "dept_time", "url"])

        # Scrape data and save to panda dataframe
        rows = table.find_all("tr")
        for row in rows[1:-1:]:

            # Catch edge case if there is no history data from the past 14 days
            if "No History Data" in row.text:
                logger.warning(f" {aircraft} has no history in the last 14 days!")
                logger.warning(f" Continuing to next aircraft...")
                return

            urls = row.find_all("a", href=True)[0]
            url = urls.get("href")
            """
            indexed column data
            [0] Date
            [1] Aircraft Type
            [2] Origin
            [3] Destination
            [4] Departure time
            [5] Arrival time
            [6] Total time
            """
            columns = row.find_all("td")
            date = columns[0].text.strip()
            try:
                # If the airport is unknown it is listed as "Near" and no airport code given.
                # In these cases, replace the airport code with "UNKW" for unknown
                if "Near" in columns[2].text:
                    origin = "UNKW"
                else:
                    origin = between_parentheses(columns[2].text)
                if "Near" in columns[3].text:
                    destination = "UNKW"
                else:
                    destination = between_parentheses(columns[3].text)
                route = origin + "-" + destination
            except TypeError:
                logger.info(f" The airplane is currently in-air! The first row of the table has to be skipped...")
                continue
            except Exception as e:
                logger.warning(f" Something went wrong while getting the plane history. ERROR: {e}")
                logger.warning(" Attempting to continue...")
                continue

            dept_time = columns[4].text
            # Catch cases where result contains "First seen"
            if dept_time[0].lower() == "f":
                logger.debug(f" dept_time: {dept_time}")
                dept_time = dept_time[11:18]
                logger.debug(f" \"First seen\" error... departure time has been corrected to: {dept_time}")
            dept_time = convert24(dept_time)

            # Convert strings into a format that will allow them to be used as table names
            date = convert_date(date)
            route = route.replace("-", "_")
            dept_time = dept_time.replace(":", "_")
            # build a row to be exported to pandas
            out = [date, route, dept_time[:-3:], url]
            # build pandas
            df.loc[len(df)] = out
        logger.info(f" {aircraft} history saved successfully!")
        return df

    except Exception as e:
        logger.critical(f" Failed to extract flight history! (flightaware_history)")
        logger.critical(f" error: {e}")
        logger.critical(f" Attempting to skip this row: {row}")
        # TODO SOLVE THE FOLLOWING ROW ERROR:
        #  <tr data-tablesaw-no-labels=""> <td colspan="7" style="text-align: center">No History Data (searched last 14 days)</td></tr>


def flightaware_getter(url):
    """
    Web scraping to grab track data from flight aware and save to pandas dataframe
    :param url: The url extracted from MySQL flight_history table, EXCLUDING flightaware.com and /track
    example: https://flightaware.com/live/flight/N81673/history/20220715/1927Z/KLXT/KAMW/tracklog
    should be given as: live/flight/N81673/history/20220715/1927Z/KLXT/KAMW
    :return: Panda dataframe containing [time, lat, long, kts, altitude]
    """

    # Make a GET request to flightaware
    url = "https://flightaware.com"+f"{url}"+"/tracklog"
    logger.info(f" Getting track data from URL: {url}")
    r = requests.get(url, timeout=5)
    # Check the status code
    if r.status_code != 200:
        logger.critical(f" Failed to connect to FlightAware! (flightaware_getter)")
        logger.critical(f" status code: {r.status_code}")
        sys.exit(r.status_code)

    # Parse the HTML
    soup = BeautifulSoup(r.text, "html.parser")
    # ------------------------------------------------------------------------------------------------------------------
    #   Extract table data
    # ------------------------------------------------------------------------------------------------------------------
    # Look for table "prettyTable fullWidth"
    try:
        table = soup.find("table", class_="prettyTable fullWidth")
        if table is None:
            raise Exception(f" Table class \"prettyTable fullWidth\" not found! {url}")
    except Exception as e:
        logger.critical(f" Error finding table on FlightAware! (flightaware_getter)")
        logger.critical(f" Attempting to continue...")
        return

    # Defining of the dataframe
    df = pd.DataFrame(columns=["time", "latitude", "longitude", "knots", "altitude"])

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
        # len(row) == 21 ensures all the data is present for a given row of data
        if len(row) == 21:
            columns = row.find_all('span', class_="show-for-medium-up")
            # len(row) columns ensures all the column elements are present
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
            builder = [time, latitude, longitude, kts, altitude]  # TODO MAKE THESE FLOATS AND INTS

        # Sometimes an empty list is generated due to scraping, reject these.
        if len(builder) == 5:
            df.loc[len(df)] = builder
    return df


def db_data_saver(aircraft):
    """
    Export the web scrapped panda dataframe into MySQL
    :param aircraft: N# of club aircraft, used for MySQL database name
    """

    # Get pandas dataframe for plane history [date, route, dept_time, url]
    hist_df = flightaware_history(aircraft)

    # catch edge case in flightaware_history, where no flight data exists from the past 14 days. Func will return None
    if hist_df is None:
        return

    # Establish connection with MySQL and initialize the cursor
    db = mysql_connect(aircraft)
    mycursor = db.cursor()

    # Create flight history parent table
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history("
                     "date DATE, "
                     "route VARCHAR(15), "
                     "dept_time VARCHAR(15), "
                     "url VARCHAR(100))")

    # Create SQLAlchemy engine to connect to MySQL Database
    user = "root"
    passwd = pw
    database = aircraft
    host_ip = '127.0.0.1'
    port = "3306"

    engine = create_engine(
        'mysql+mysqlconnector://' + user + ':' + passwd + '@' + host_ip + ':' + port + '/' + database,
        echo=False)

    try:
        # Convert dataframe to sql table (flight_history)
        hist_df.to_sql('flight_history', engine, if_exists="append", index=False)
    except Exception as e:
        logger.critical(" An error occurred with the SQLAclhemy engine! (db_data_saver)")
        logger.critical(f" Error: {e}")
        sys.exit(e)

    # Delete duplicate data, since we are using if_exists="append" from above
    # Reference: https://phoenixnap.com/kb/mysql-remove-duplicate-rows#ftoc-heading-8
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history_temp SELECT DISTINCT date, route, dept_time, url "
                     "FROM flight_history")
    mycursor.execute("DROP TABLE flight_history")
    mycursor.execute("ALTER TABLE flight_history_temp RENAME TO flight_history")

    # Create track data tables using rows from flight_history
    mycursor.execute("SELECT * FROM flight_history")
    new_hist = []
    for x in mycursor:
        # convert DATE format into a string with underscores to allow to be used as table name
        date = str(x[0])
        date = date.replace("-", "_")
        hour = x[2]
        hour = hour[0:2:]
        new_hist.append(date + "__" + x[1].lower() + "__" + hour)

    # Find which tables do not yet exist in the database by comparing new history and database flight_history lists
    tables_exist = []
    mycursor.execute("SHOW TABLES")
    res = mycursor.fetchall()
    for x in res:
        tables_exist.append(x[0])
    hist = [x for x in new_hist if x not in tables_exist]

    # Exit condition if there are no new flights to add to the database
    if not hist:
        logger.info(f" {aircraft} has no new flights to add to the database!")
        logger.info(f" Continuing...")
        sleep(3)
        return

    # Build new flight details tables
    for name in hist:
        name = name.lower()
        try:
            # Create a flight details CHILD table
            mycursor.execute(f"CREATE TABLE {name}("
                             "time MEDIUMINT(10), "
                             "latitude FLOAT, "
                             "longitude FLOAT, "
                             "knots MEDIUMINT(5), "
                             "altitude MEDIUMINT(5))")
        except Exception as e:
            logger.warning(f" Error while attempting to create table {name}")
            logger.warning(e)
            logger.warning(f" Attempting to continue...")
            continue

    try:
        mycursor.execute("SELECT * FROM flight_history")
        name = []
        url_list = []
        for x in mycursor:
            # convert DATE format to string with underscores to allow to be used as table name
            date = str(x[0])
            date = date.replace("-", "_")
            hour = x[2]
            hour = hour[0:2:]
            name.append(date + "__" + x[1] + "__" + hour)
            url_list.append(x[3])
    except Exception as e:
        db.close()
        logger.critical(" An error occurred while trying to build the URL list! (db_data_saver)")
        logger.critical(e)
        sys.exit()

    if len(url_list) != len(name):
        logger.critical(f" Length of names and length of url_list are not the same!")
        sys.exit(f" length of names and length of url_list are not the same!")

    # relate the name and url_list lists together and compare vs tables_exist to determine if new data is needed
    # will create new_flights that only contains urls of "new" flights relative to the database history
    flightaware_combined_hist = dict(zip(name, url_list))
    new_flights = []
    for new_leg in flightaware_combined_hist.keys():
        if new_leg.lower() in hist:
            new_flights.append(flightaware_combined_hist[new_leg])
            logger.info(f" New leg found: {new_leg}")

    # try to get specific history data from each url page
    logger.info(" Attempting to get flight details...")
    for i in range(len(new_flights)):
        try:
            details_df = flightaware_getter(new_flights[i])
            if details_df is None:
                continue
            # Convert dataframe to sql table (flight details)
            details_df.to_sql(name[i].lower(), engine, if_exists="replace", index=False)
            logger.info(f" {i+1} out of {len(new_flights)} completed!")
            if i != len(new_flights)-1:
                logger.info(" Waiting 3 seconds...")
                sleep(3)
        except Exception as e:
            logger.warning(f" An error occurred while trying to populate the flight data tables! (db_data_saver)")
            logger.warning(f" Error: {e}")
            logger.warning(" Waiting 3 seconds...")
            sleep(3)
    logger.info(f" Tables built successfully!")
    db.close()


def db_data_getter(aircraft, month):
    """
    Import the data from MySQL and convert into pandas dataframe
    :return: pandas dataframe
    """
    # Establish connection with MySQL and init cursor
    db = mysql_connect(aircraft)
    mycursor = db.cursor()

    # Create SQLAlchemy engine to connect to MySQL Database
    user = "root"
    passwd = pw
    database = aircraft
    host_ip = '127.0.0.1'
    port = "3306"

    engine = create_engine(
        'mysql+mysqlconnector://' + user + ':' + passwd + '@' + host_ip + ':' + port + '/' + database,
        echo=False)

    # Use the flight history table
    try:
        mycursor.execute(f"SELECT * FROM flight_history "
                         f"WHERE month(date)={month}")
        hist = []
        for x in mycursor:
            # convert DATE format to string with underscores to allow to be used as table name
            date = str(x[0])
            date = date.replace("-", "_")
            hour = x[2]
            hour = hour[0:2:]
            hist.append(date + "__" + x[1].lower() + "__" + hour)
    except Exception as e:
        db.close()
        logger.critical(" An error occurred while grabbing the flight history table names! (db_data_getter)")
        logger.critical(e)
        sys.exit(e)

    # Defining of the dataframe
    total_df = pd.DataFrame()

    # for each piece of history, get the flight data
    try:
        for leg in hist:
            query = f"SELECT * FROM {leg}"
            res_df = pd.read_sql(query, engine)
            if res_df.empty:
                continue
            total_df = pd.concat([total_df, res_df], ignore_index=True)
    except Exception as e:
        logger.warning(f" Error while grabbing {leg}: {e}")
        logger.warning(f" Attempting to continue...")

    return total_df


def calculate_stats(aircraft):  # TODO UPDATE WITH NEW TABLE NAMES
    """ Calculate various stats related to the aircraft's history"""
    # Establish connection with MySQL:
    db = mysql_connect(aircraft)

    def dist_travelled():
        """
        Calculate the total distance travelled by the aircraft using lat/long data.
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
        mycursor.execute("SELECT Latitude, Longitude FROM flight")

        latitude = []
        longitude = []
        for x in mycursor:
            latitude.append(x[0])
            longitude.append(x[1])

        total_dist = 0
        for x in range(len(latitude[:-1:])):
            total_dist += lat_long_dist(latitude[x], latitude[x + 1], longitude[x], longitude[x + 1])
        print(f" The total distance travelled was {round(total_dist, 2)} Miles")
        return total_dist

    def time_aloft():
        """
        Calculate the max time aloft by using the aircraft in-air data
        :return: time aloft as a timedelta format
        """
        # initialize the MySQL cursor
        mycursor = db.cursor()

        # grab the latitude and longitude data from MySQL
        mycursor.execute("SELECT Time FROM flight")

        time = []
        for x in mycursor:
            time.append(x)

        start_time = time[0]
        end_time = time[-1]
        time_delta = end_time[0] - start_time[0]

        def strfdelta(tdelta, fmt):
            """
            Takes timedelta and returns a format that can be used to print hours and minutes
            :param tdelta: flight time (end time - start time) in timedelta format
            :return: a format that allows reporting of {hours}, {minutes}, and {seconds}
            """
            d = {"days": tdelta.days}
            d["hours"], rem = divmod(tdelta.seconds, 3600)
            d["minutes"], d["seconds"] = divmod(rem, 60)
            return fmt.format(**d)

        print(strfdelta(time_delta, " The trip took {hours} hours and {minutes} minutes"))
        return time_delta

    def airports_visited():
        """Determine the airports visited"""
        pass

    # calculate_stats function calls
    dist_travelled()
    time_aloft()
    airports_visited()

    db.close()


def state_plotter(states, us_map=True):
    """
    Return ax to be used with geopandas
    :param states: States to be mapped
    :param us_map: True if CONUS, False if "zoomed in". True will result in "highlighted" states
    :return:
    """
    usa = gpd.read_file("states_21basic/states.shp")

    fig, ax = plt.subplots(figsize=(10, 10))

    if us_map:
        usa[1:50].plot(ax=ax, alpha=0.3)

        for n in states:
            usa[usa.STATE_ABBR == f"{n}"].plot(ax=ax, edgecolor="y", linewidth=2)

    elif not us_map:
        for n in states:
            usa[usa.STATE_ABBR == f"{n}"].plot(ax=ax, edgecolor="y", linewidth=2, alpha=0.3, linestyle="--")
    return ax


def local_area_map(fleet, area, month):
    """Use the lat/long data to plot a composite map of the KC area
    # TODO ADD DOCSTRING
    """

    # Define the map # todo add new text box and get() states.
    ax = state_plotter(area, us_map=False)

    # N81673 Archer
    if "N81673" in fleet:
        df_N81673 = db_data_getter("N81673", month)
        # Catch condition where there are is no flight history
        if not df_N81673.empty:
            geom_N81673 = [Point(xy) for xy in zip(df_N81673["longitude"].astype(float), df_N81673["latitude"].astype(float))]
            gdf_N81673 = GeoDataFrame(df_N81673, geometry=geom_N81673)
            gdf_N81673.plot(ax=ax, color="red", markersize=5, label="Archer - N81673", linestyle="-")

    # N3892Q C172 (OJC)
    if "N3892Q" in fleet:
        df_N3892Q = db_data_getter("N3892Q", month)
        # Catch condition where there are is no flight history
        if not df_N3892Q.empty:
            geom_N3892Q = [Point(xy) for xy in zip(df_N3892Q["longitude"].astype(float), df_N3892Q["latitude"].astype(float))]
            gdf_N3892Q = GeoDataFrame(df_N3892Q, geometry=geom_N3892Q)
            gdf_N3892Q.plot(ax=ax, color="blue", markersize=5, label="C172 - N3892Q")

    # N20389 C172 (OJC)
    if "N20389" in fleet:
        df_N20389 = db_data_getter("N20389", month)
        # Catch condition where there are is no flight history
        if not df_N20389.empty:
            geom_N20389 = [Point(xy) for xy in zip(df_N20389["longitude"].astype(float), df_N20389["latitude"].astype(float))]
            gdf_N20389 = GeoDataFrame(df_N20389, geometry=geom_N20389)
            gdf_N20389.plot(ax=ax, color="green", markersize=5, label="C172 - N20389")

    # N182WK C182 (LXT)  # TODO UPDATE THESE CALL CONDITIONS TO INCLUDE NO FLIGHT HISTORY (ex: no flights in August)
    if "N182WK" in fleet:
        df_N182WK = db_data_getter("N182WK", month)
        # Catch condition where there are is no flight history
        if not df_N182WK.empty:
            geom_N182WK = [Point(xy) for xy in zip(df_N182WK["longitude"].astype(float), df_N182WK["latitude"].astype(float))]
            gdf_N182WK = GeoDataFrame(df_N182WK, geometry=geom_N182WK)
            gdf_N182WK.plot(ax=ax, color="cyan", markersize=5, label="C182 - N182WK")

    # N58843 C182 (LXT)
    if "N58843" in fleet:
        df_N58843 = db_data_getter("N58843", month)
        # Catch condition where there are is no flight history
        if not df_N58843.empty:
            geom_N58843 = [Point(xy) for xy in zip(df_N58843["longitude"].astype(float), df_N58843["latitude"].astype(float))]
            gdf_N58843 = GeoDataFrame(df_N58843, geometry=geom_N58843)
            gdf_N58843.plot(ax=ax, color="white", markersize=5, label="C182 - N58843")

    # N82145 Saratoga
    if "N82145" in fleet:
        df_N82145 = db_data_getter("N82145", month)
        # Catch condition where there are is no flight history
        if not df_N82145.empty:
            geom_N82145 = [Point(xy) for xy in zip(df_N82145["longitude"].astype(float), df_N82145["latitude"].astype(float))]
            gdf_N82145 = GeoDataFrame(df_N82145, geometry=geom_N82145)
            gdf_N82145.plot(ax=ax, color="black", markersize=5, label="Saratoga - N82145")

    # N4803P Debonair
    if "N4803P" in fleet:
        df_N4803P = db_data_getter("N4803P", month)
        # Catch condition where there are is no flight history
        if not df_N4803P.empty:
            geom_N4803P = [Point(xy) for xy in zip(df_N4803P["longitude"].astype(float), df_N4803P["latitude"].astype(float))]
            gdf_N4803P = GeoDataFrame(df_N4803P, geometry=geom_N4803P)
            gdf_N4803P.plot(ax=ax, color="magenta", markersize=5, label="Debonair - N4803P")

    # finally, plot
    plt.legend(loc="upper right")
    plt.show()
    pass


def conus_area_map():
    """Use the lat/long data to plot a composite map of the CONUS"""
    pass


def main():
    """Main entry point for the script."""

    #-------------------------------------------------------------------------------------------------------------------
    #                                                   TKINTER GUI WINDOW
    #                                       Reference https://www.pythontutorial.net/tkinter
    #-------------------------------------------------------------------------------------------------------------------

    # establish root as the main window
    root = tk.Tk()
    root.title('FCKC Track Log')
    root.geometry('930x520+200+200')
    root.resizable(False, False)

    # Bring the window to the top of the screen
    root.attributes('-topmost', True)
    root.update()
    root.attributes('-topmost', False)

    fleet = ("N81673 - Archer",
             "N2389Q - C172",
             "N20389 - C172",
             "N182WK - C182",
             "N58843 - C182",
             "N82145 - Saratoga",
             "N4803P - Debonair")

    def check_pw():
        # Check if the PW has been set. If not, get PW with mysql_connect()
        try:
            pw
        except NameError:
            mysql_connect()

    def error_none_selected():
        # create message box that contains the error if no aircraft were selected
        none_select = tk.Toplevel(root)
        none_select.title("Error!")
        none_select.resizable(False, False)

        # Position message box to be coordinated with the root window
        root_x = root.winfo_rootx()
        root_y = root.winfo_rooty()
        win_x = root_x + 300
        win_y = root_y + 100
        none_select.geometry(f'+{win_x}+{win_y}')

        # create the label on the message box
        prog_msg = tk.Label(none_select, text=f" Error, no aircraft selected.")
        prog_msg.grid(
            column=1,
            row=0,
            pady=10,
            sticky="S")

        # create button that closes the error box
        close_button = ttk.Button(
            none_select,
            text='Close',
            command=none_select.destroy)
        close_button.grid(
            column=1,
            row=1,
            sticky="N")

    def mysql_connect():
        # create message box to take in the MySQL database password
        connector = tk.Toplevel(root)
        connector.title("MySQL Connect")
        connector.resizable(False, False)

        # Position message box to be coordinated with the root window
        root_x = root.winfo_rootx()
        root_y = root.winfo_rooty()
        win_x = root_x + 300
        win_y = root_y + 100
        connector.geometry(f'+{win_x}+{win_y}')

        # TEXT: create the label on the message box
        connect_text = tk.Label(connector,
                                text=f" Please enter the MySQL database password: ")
        connect_text.grid(
            column=1,
            row=0,
            pady=20)

        # Get the password using ENTRY
        pass_text = tk.Entry(connector, show="*")
        pass_text.grid(
            column=1,
            row=1,
            padx=25)

        # BUTTON: Cancel button
        cancel_button = ttk.Button(
            connector,
            text='Cancel',
            command=connector.destroy)
        cancel_button.grid(
            column=1,
            row=3,
            sticky="W",
            pady=5,
            padx=5)

        # BUTTON: Connect to MySQL
        connect_button = ttk.Button(
            connector,
            text='Connect',
            command=lambda: mysql_dummy())
        connect_button.grid(
            column=1,
            row=3,
            sticky="E",
            pady=5,
            padx=5)

        def mysql_dummy():
            # create the global variable pw and get it from the pass_text entry widget
            global pw
            pw = pass_text.get()

            # log output
            log_output.configure(state="normal")  # allow editing of the log
            log_output.insert(tk.END, f" Attempting to connect to MySQL...\n\n")

            # Test the database connection
            try:
                # Init connection to MySQL database
                db = mysql.connector.connect(
                    host="localhost",
                    user="root",
                    passwd=pw,
                )
                log_output.insert(tk.END, f" Connection successful!\n\n")
            except Exception as e:
                logger.critical(f" database connection failed! (mysql_dummy)")
                log_output.insert(tk.END, f" Incorrect password, please try again.\n\n")
            else:
                connector.destroy()
                db.close()  # close db connection, as it was only used to test the password

            # Always scroll to the index: "end"
            log_output.see(tk.END)
            log_output.configure(state="disabled")  # disable editing of the log

        # Wait for the window to close before continuing.
        # This is most useful when "interrupting" other functions to ask for the password to be entered.
        connector.wait_window(connector)

    def get_aircraft_data():
        check_pw()

        # get selected indices
        selected_indices = fleet_listbox.curselection()
        # get selected items using indices
        selected_aircraft = [fleet_listbox.get(i) for i in selected_indices]
        # Remove the excess information from the selectable listbox data
        for i, x in enumerate(selected_aircraft):
            selected_aircraft[i] = x.split("-")[0].strip()
        if not selected_aircraft:
            error_none_selected()
            return

        selected_aircraft_str = "\n".join(selected_aircraft)

        # create message box that contains a progress bar on the status of the fleet
        aircraft_progress = tk.Toplevel(root)
        aircraft_progress.title("Data Gathering Progress")
        aircraft_progress.resizable(False, False)

        # Bring the window to the top of the screen
        aircraft_progress.attributes('-topmost', True)
        aircraft_progress.update()
        aircraft_progress.attributes('-topmost', False)

        # Position message box to be coordinated with the root window
        root_x = root.winfo_rootx()
        root_y = root.winfo_rooty()
        win_x = root_x + 250
        win_y = root_y + 50
        aircraft_progress.geometry(f'+{win_x}+{win_y}')

        # Configure columns/rows
        aircraft_progress.columnconfigure(1, weight=1)
        aircraft_progress.rowconfigure(1, weight=1)

        # create the label on the message box
        prog_msg = tk.Label(aircraft_progress,
                            text=f" Getting aircraft data for: \n{selected_aircraft_str}")
        prog_msg.grid(column=1, row=0)

        # create the progressbar
        pb = ttk.Progressbar(
            aircraft_progress,
            orient='horizontal',
            mode='indeterminate',
            length=280)

        # place the progressbar
        pb.grid(column=1, row=1, columnspan=2, padx=10, pady=20)
        pb.start()

        # BUTTON: cancel data gathering
        data_cancel_button = ttk.Button(
            aircraft_progress,
            text="Cancel",
            command=lambda: data_cancel())
        data_cancel_button.grid(
            column=1,
            row=2)


        # TODO THREADING
        # Call data gathering
        for aircraft in selected_aircraft:
            logger.info(f" ~~~~~~~~~~~~~ {aircraft} ~~~~~~~~~~~~~")
            db_data_saver(aircraft)
            logger.info(f"\n")
            # if aircraft == selected_aircraft[-1]:
            #     log_output.configure(state="normal")  # allow editing of the log
            #     log_output.insert(tk.END, f"Data gathering completed!\n\n")
            #     aircraft_progress.destroy()
            #     # Always scroll to the index: "end"
            #     log_output.see(tk.END)
            #     log_output.configure(state="disabled")  # disable editing of the log
            # else:
            #     sleep(1)

        def data_cancel():
            log_output.configure(state="normal")  # allow editing of the log
            log_output.insert(tk.END, f"Data gathering has been cancelled!\n\n")
            aircraft_progress.destroy()
            # Always scroll to the index: "end"
            log_output.see(tk.END)
            log_output.configure(state="disabled")  # disable editing of the log

    def graph_aircraft():
        check_pw()

        # get selected indices
        selected_indices = fleet_listbox.curselection()
        # get selected items
        sel_aircraft = [fleet_listbox.get(i) for i in selected_indices]
        # Remove the excess information from the selectable table data
        for i, x in enumerate(sel_aircraft):
            sel_aircraft[i] = x.split("-")[0].strip()
        if not sel_aircraft:
            error_none_selected()
            return
        sel_aircraft_str = "   ".join(sel_aircraft)

        # Call graphing function
        states_list = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
                       "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
                       "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
                       "WI", "WY"]
        states_area = states_text.get("1.0", "end")

        try:
            states_area = states_area.split(",")
            states_area = [x.strip() for x in states_area]
            for state in states_area:
                if state not in states_list:
                    logger.warning(f" State ({state}) entered not in the states_list (graph_aircraft)")
                    return
        except Exception as e:
            logger.warning(f" Something went wrong with graph_aircraft, splitting of the states.")

        month_dates = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12}

        # get the current month from the month combobox
        sel_month = month_cb.get()

        # convert the month to a number
        sel_month = month_dates[sel_month]

        # call the grapher
        local_area_map(sel_aircraft, states_area, sel_month)

        # log the commands
        log_output.configure(state="normal")  # allow editing of the log
        log_output.insert(tk.END,
                          f" \nA local graph with the following aircraft has been created:\n {sel_aircraft_str}")
        log_output.insert(tk.END, f"\n\n")
        # Always scroll to the index: "end"
        log_output.see(tk.END)
        log_output.configure(state="disabled")  # disable editing of the log

    def calculate_stats_placeholder():
        # TODO placeholder until calculate_stats is scrabbed
        check_pw()

        # log the commands
        log_output.configure(state="normal")  # allow editing of the log
        log_output.insert(tk.END, f"\n Stats! Stats! Stats!")
        log_output.insert(tk.END, f"\n")
        # Always scroll to the index: "end"
        log_output.see(tk.END)
        log_output.configure(state="disabled")  # disable editing of the log

    def clear_log():
        log_output.configure(state="normal")  # allow editing of the log
        log_output.delete("1.0", tk.END)
        log_output.configure(state="disabled")  # disable editing of the log

    def url_data_getter():
        """
        Single-use URL grabber to allow specific flights to be added to the database
        """
        # get the data entered in url_text
        entered_url = url_text.get("1.0", "end")
        # TODO ADD CHECK FOR INVALID URL
        # Catch no URL error condition
        if len(entered_url) == 1:
            logger.warning(f" No URL has been entered!")
            # log the commands
            log_output.configure(state="normal")  # allow editing of the log
            log_output.insert(tk.END, f"\n No URL has been entered!")
            log_output.insert(tk.END, f"\n")
            # Always scroll to the index: "end"
            log_output.see(tk.END)
            log_output.configure(state="disabled")  # disable editing of the log
            return

        # strip any potential extra pieces to properly call flightaware_getter
        entered_url = entered_url.replace("https://flightaware.com", "")
        entered_url = entered_url.replace("flightaware.com", "")
        entered_url = entered_url.replace("/tracklog", "")

        # get the data needed for the history table: date, route, dept_time using the entered url
        split_url = entered_url.split("/")
        db_name = split_url[3]
        date = split_url[5]
        time = split_url[6]
        time = time[0:2] + "_" + time[2:4]
        route = split_url[7] + "_" + split_url[8]
        route = route[0:-1]
        date = date[0:4] + "-" + date[4:6] + "-" + date[6:8]

        new_hist = [date, route, time, entered_url[:-1]]
        # convert from list to df to easier save to MySQL
        new_hist_df = pd.DataFrame([new_hist], columns=["date", "route", "dept_time", "url"])

       # Check if password exists
        check_pw()
        # Create SQLAlchemy engine to connect to MySQL Database
        user = "root"
        passwd = pw
        database = db_name
        host_ip = '127.0.0.1'
        port = "3306"
        engine = create_engine(
            'mysql+mysqlconnector://' + user + ':' + passwd + '@' + host_ip + ':' + port + '/' + database,
            echo=False)
        try:
            # Convert dataframe to sql table (flight_history)
            new_hist_df.to_sql('flight_history', engine, if_exists="append", index=False)
        except Exception as e:
            logger.critical(" An error occurred with the SQLAclhemy engine! (db_data_saver)")
            logger.critical(f" Error: {e}")
            sys.exit(e)

        # make table name
        date = date.replace("-", "_")
        hour = time[0:2:]
        table_name = date + "__" + route.lower() + "__" + hour

        # create new MySQL table and populate with data
        try:
            # Init connection to MySQL database
            db = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd=pw,
                database=db_name)
            logger.info(f" Database connection to {db_name} successful.")
        except Exception as e:
            logger.critical(f" {db_name} database connection failed! (mysql_connect)")
            sys.exit(e)
        mycursor = db.cursor()

        # Build new flight details tables
        try:
            # Create a flight details CHILD table
            mycursor.execute(f"CREATE TABLE {table_name}("
                             "time MEDIUMINT(10), "
                             "latitude FLOAT, "
                             "longitude FLOAT, "
                             "knots MEDIUMINT(5), "
                             "altitude MEDIUMINT(5))")
        except Exception as e:
            logger.warning(f" Error while attempting to create table {table_name}")
            logger.warning(e)

        # get the flight details
        details_df = flightaware_getter(entered_url)
        try:
            # Convert dataframe to sql table (flight details)
            details_df.to_sql(table_name, engine, if_exists="replace", index=False)
        except Exception as e:
            logger.warning(f" An error occurred while trying to populate the flight data tables! (db_data_saver)")
            logger.warning(f" Error: {e}")

        logger.info(f" Table built successfully!")
        db.close()

        # log the commands
        log_output.configure(state="normal")  # allow editing of the log
        log_output.insert(tk.END, f"\n {entered_url} has been successfully uploaded to the DB!")
        log_output.insert(tk.END, f"\n")
        # Always scroll to the index: "end"
        log_output.see(tk.END)
        log_output.configure(state="disabled")  # disable editing of the log

    def midwest_state_acronyms():
        """
        Enter Midwestern state acronyms into the state plotter text box
        """
        states_text.insert("1.0", "MO, KS, IA, MN, IL, WI, NE")

    # define the row where the main buttons are
    bot_button_row = 4

    # COMBOBOX: Select month
    selected_month = tk.StringVar()
    month_cb = ttk.Combobox(root, textvariable=selected_month)
    # prevent typing a value
    month_cb["state"] = "readonly"
    # set values
    month_cb["values"] = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
                          "October", "November", "December"]
    month_cb.grid(
        column=0,
        row=1,
        padx=5,
        sticky="N")

    # LABEL: Select month
    sel_month_lab = tk.Label(root, text="Select month:")
    sel_month_lab.grid(
        column=0,
        row=0,
        pady=5)
    # Set the default value to the current month
    current_month = datetime.now().strftime("%B")
    month_cb.set(current_month)

    # LABEL: select aircraft
    fleet_lab = ttk.Label(root, text="Select aircraft:")
    fleet_lab.grid(
        column=0,
        row=1,
        sticky="S")

    # LISTBOX: to select which aircraft to manipulate
    fleet_var = tk.StringVar(value=fleet)
    fleet_listbox = tk.Listbox(
        root,
        listvariable=fleet_var,
        height=7,
        selectmode="extended")
    fleet_listbox.grid(
        column=0,
        row=2,
        sticky="N")

    # BUTTON: Connect to MySQL Database
    connect_mysql = ttk.Button(
        root,
        text="Connect to MySQL",
        command=lambda: mysql_connect())
    connect_mysql.grid(
        column=0,
        row=bot_button_row,
        padx=25)

    # BUTTON: Get flight history
    aircraft_button = ttk.Button(
        root,
        text="Get flight history",
        command=lambda: get_aircraft_data())
    aircraft_button.grid(
        column=2,
        row=bot_button_row,
        sticky="E")

    # BUTTON: Clear log
    clear_log_button = ttk.Button(
        root,
        text="Clear text log",
        command=lambda: clear_log())
    clear_log_button.grid(
        column=1,
        row=bot_button_row)

    # BUTTON: Calculate stats
    stats_button = ttk.Button(
        root,
        text="Calculate stats",
        command=lambda: calculate_stats_placeholder())
    stats_button.grid(
        column=5,
        row=bot_button_row)

    # LABEL: output log
    output_lab = ttk.Label(root, text="Output log", font=("Helvetica", 12))
    output_lab.grid(
        column=1,
        row=0,
        sticky="SW",
        padx=25)

    # TEXT: Output log
    log_output = ScrolledText(root, height=15, width=65)
    log_output.grid(
        column=1,
        row=1,
        columnspan=3,
        rowspan=2,
        padx=25)
    # Disable editing of the output log. state="normal" will have to be called prior to every edit
    log_output.configure(state="disabled")

    # TEXT : URL txt input
    url_text = tk.Text(root, height=2, width=60)
    url_text.grid(
        column=2,
        row=5,
        rowspan=1,
        columnspan=2)

    # BUTTON: Get data from URL
    url_button = ttk.Button(
        root,
        text="Grab URL data",
        command=lambda: url_data_getter())
    url_button.grid(
        column=1,
        row=5,
        sticky="E",
        pady=15)

    # TEXT : States text input
    states_text = tk.Text(root, height=2, width=60)
    states_text.grid(
        column=2,
        row=6,
        rowspan=1,
        columnspan=2)

    # BUTTON: Create local graph
    aircraft_button = ttk.Button(
        root,
        text="Create local graph",
        command=lambda: graph_aircraft())
    aircraft_button.grid(
        column=1,
        row=6,
        sticky="E",
        pady=15)

    # BUTTON: Enter midwestern states in local graph text box
    midwest_button = ttk.Button(
        root,
        text="Midwest States",
        command=lambda: midwest_state_acronyms())
    midwest_button.grid(
        column=2,
        row=7)

    # Execute
    root.mainloop()

    logger.info(" Code complete.")


if __name__ == "__main__":
    sys.exit(main())
