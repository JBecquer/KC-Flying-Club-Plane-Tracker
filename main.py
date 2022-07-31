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
import time
import logging


# create logger (copied from https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial)
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
    :param s:
    :return:
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


def flightaware_history(aircraft):
    """
    Grab the aircraft history from flight aware and return pandas dataframe.
    :param aircraft: aircraft ID. ex: N182WK
    :type aircraft: str
    :return: pandas df = [date, route, dept_time, URL]
    """
    headers = {
        'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Accept-Language': "en-US,en;q=0.9",
        'Referer': "https://google.com",
        "DNT": "1"
    }
    # Make a GET request to flightaware
    url = f"https://flightaware.com/live/flight/{aircraft}/history/80"
    logger.info(f" Getting plane history from: {url}")
    r = requests.get(url, headers=headers)
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

        # Scrape data
        rows = table.find_all("tr")
        for row in rows[1:-1:]:
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
            except Exception as e:
                logger.warning(f" Something went wrong while getting the plane history: {e}")
                logger.warning(" Attempting to continue...")
                continue

            dept_time = convert24(columns[4].text)

            # Convert strings into a format that will allow them to be used as table names
            date = date.replace("-", "_")
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
        pass


def flightaware_getter(url):
    """
    Web scraping to grab data from flight aware and save to file.
    :return: Panda dataframe containing list[time, lat, long, kts, altitude]
    """

    # Make a GET request to flightaware
    url = "https://flightaware.com"+f"{url}"+"/tracklog"
    logger.info(f" URL: {url}")
    r = requests.get(url)
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
        sys.exit(e)

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
            builder = [time, latitude, longitude, kts, altitude]  # TODO MAKE THESE FLOATS AND INTS

        # Sometimes an empty list is generated due to scraping, reject these.
        if len(builder) == 5:
            df.loc[len(df)] = builder
    return df


def db_data_saver(aircraft):
    """
    Export the web scrapped panda dataframe into MySQL
    :param aircraft: N# of club aircraft, used for MySQL Schema
    """

    # Get pandas dataframe for PLANE HISTORY
    hist_df = flightaware_history(aircraft)

    # Establish connection with MySQL and initialize the cursor
    db = mysql_connect(aircraft)
    mycursor = db.cursor()

    # Create flight history PARENT table
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history("
                     "date VARCHAR(15), "
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
        # Convert dataframe to sql table (FLIGHT HISTORY)  # TODO FIND WAY TO ONLY REPLACE IF NOT EXISTS
        hist_df.to_sql('flight_history', engine, if_exists="replace", index=False)
    except Exception as e:
        logger.critical(" An error occured with the SQLAclhemy engine! (db_data_saver)")
        logger.critical(f" Error: {e}")
        sys.exit(e)

    # Create individual flight history tables
    mycursor.execute("SELECT * FROM flight_history")
    hist = []
    for x in mycursor:
        hour = x[2]
        hour = hour[0:2:]
        hist.append(x[0] + "__" + x[1] + "__" + hour)

    for name in hist:
        name = name.lower()
        try:
            # Create a flight details CHILD table
            mycursor.execute(f"CREATE TABLE IF NOT EXISTS {name}("
                             "time TIME, "
                             "latitude FLOAT, "
                             "longitude FLOAT, "
                             "knots MEDIUMINT(5), "
                             "altitude MEDIUMINT(5))")
        except Exception as e:
            logger.warning(f" Database table {name} already exists!")
            logger.warning(f" Attempting to continue...")
            continue

    try:
        mycursor.execute("SELECT * FROM flight_history")
        url_list = []
        for x in mycursor:
            url_list.append(x[3])

        mycursor.execute("SELECT * FROM flight_history")
        name = []
        for x in mycursor:
            hour = x[2]
            hour = hour[0:2:]
            name.append(x[0] + "__" + x[1] + "__" + hour)
    except Exception as e:
        db.close()
        logger.critical(" An error occurred while trying to build the URL list! (db_data_saver)")
        logger.critical(e)
        sys.exit(e)

    if len(url_list) != len(name):
        logger.critical(f" length of names and length of url_list are not the same!")
        sys.exit(f" length of names and length of url_list are not the same!")

    # try to get specific history data from each url page
    logger.info(" Attempting to get flight details...")
    for i in range(len(url_list)):
        try:
            details_df = flightaware_getter(url_list[i])
            # Convert dataframe to sql table (flight details)
            details_df.to_sql(name[i].lower(), engine, if_exists="replace", index=False)
            logger.info(f" {i+1} out of {len(url_list)} completed!")
            if i != len(url_list)-1:
                logger.info(" Waiting 3 seconds...")
                time.sleep(3)
        except Exception as e:
            logger.warning(f" An error occurred while trying to populate the flight data tables! (db_data_saver)")
            logger.warning(f" Error: {e}")
            logger.warning(" Waiting 3 seconds...")
            time.sleep(3)
    logger.info(f" Tables built successfully!")
    db.close()


def db_data_getter(aircraft):
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

    try:
        mycursor.execute("SELECT * FROM flight_history")
        hist = []
        for x in mycursor:
            hour = x[2]
            hour = hour[0:2:]
            hist.append(x[0] + "__" + x[1] + "__" + hour)
    except Exception as e:
        db.close()
        logger.critical(" An error occurred while grabbing the flight history table names! (db_data_getter)")
        logger.critical(e)
        sys.exit(e)

    # Defining of the dataframe
    total_df = pd.DataFrame()

    try:
        for leg in hist:
            query = f"SELECT * FROM {leg}"
            res_df = pd.read_sql(query, engine)
            if res_df.empty:
                continue
            total_df = pd.concat([total_df, res_df], ignore_index=True)
    except Exception as e:
        logger.warning(f" {leg} not found! Attempting to continue...")

    return total_df


def calculate_stats(aircraft):
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
            usa[usa.STATE_ABBR == f"{n}"].plot(ax=ax, edgecolor="y", linewidth=2)
    return ax


def local_area_map():
    """Use the lat/long data to plot a composite map of the KC area"""

    # TODO UPDATE ERROR CONDITIONS TO PASS OVER NO HISTORY DATA
    #  (An error occured with the SQLAclhemy engine! (db_data_saver))

    df_N81673 = db_data_getter("N81673")
    df_N3892Q = db_data_getter("N3892Q")
    df_N20389 = db_data_getter("N20389")
    # df_N182WK = db_data_getter("N182WK")
    # df_N58843 = db_data_getter("N58843")
    df_N82145 = db_data_getter("N82145")
    # df_N4803P = db_data_getter("N4803P")

    # grab the latitude and longitude data from the panda dataframe
    geom_N81673 = [Point(xy) for xy in zip(df_N81673["longitude"].astype(float), df_N81673["latitude"].astype(float))]
    geom_N3892Q = [Point(xy) for xy in zip(df_N3892Q["longitude"].astype(float), df_N3892Q["latitude"].astype(float))]
    geom_N20389 = [Point(xy) for xy in zip(df_N20389["longitude"].astype(float), df_N20389["latitude"].astype(float))]
    # geom_N182WK = [Point(xy) for xy in zip(df_N182WK["longitude"].astype(float), df_N182WK["latitude"].astype(float))]
    # geom_N58843 = [Point(xy) for xy in zip(df_N58843["longitude"].astype(float), df_N58843["latitude"].astype(float))]
    geom_N82145 = [Point(xy) for xy in zip(df_N82145["longitude"].astype(float), df_N82145["latitude"].astype(float))]
    # geom_N4803P = [Point(xy) for xy in zip(df_N4803P["longitude"].astype(float), df_N4803P["latitude"].astype(float))]

    gdf_N81673 = GeoDataFrame(df_N81673, geometry=geom_N81673)
    gdf_N3892Q = GeoDataFrame(df_N3892Q, geometry=geom_N3892Q)
    gdf_N20389 = GeoDataFrame(df_N20389, geometry=geom_N20389)
    # gdf_N182WK = GeoDataFrame(df_N182WK, geometry=geom_N182WK)
    # gdf_N58843 = GeoDataFrame(df_N58843, geometry=geom_N58843)
    gdf_N82145 = GeoDataFrame(df_N82145, geometry=geom_N82145)
    # gdf_N4803P = GeoDataFrame(df_N4803P, geometry=geom_N4803P)

    ax = state_plotter(["MO", "KS", "MN", "WI", "IL", "NE", "IA"], us_map=False)

    gdf_N81673.plot(ax=ax, color="red", markersize=5)
    gdf_N3892Q.plot(ax=ax, color="blue", markersize=5)
    gdf_N20389.plot(ax=ax, color="green", markersize=5)
    # gdf_N182WK.plot(ax=ax, color="cyan", markersize=5)
    # gdf_N58843.plot(ax=ax, color="white", markersize=5)
    gdf_N82145.plot(ax=ax, color="black", markersize=5)
    # gdf_N4803P.plot(ax=ax, color="magenta", markersize=5)
    plt.legend(['N81673 - Archer',
                'N3892Q - C172',
                'N20389 - C172',
                'N82145 - Saratoga'])
    plt.show()
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
        # "N182WK",  # C182
        # "N58843",  # C182
        "N82145",  # Saratoga
        "N4803P"  # Debonair  # TODO NEED TO TROUBLESHOOT THIS AIRCRAFT
    ]
    # flightaware_getter()  # DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    # flightaware_history("N81673")  #  DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    # db_data_getter(aircraft)  # DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    # calculate_stats(aircraft)  # TODO NEED TO SCRUB
    # for aircraft in fleet[1:2]:
    #     db_data_saver(aircraft)

    local_area_map()

    logging.info(" Code complete.")


# Make pw a global variable so it can be accessed by all the various database calls
pw = getpass(" Enter MySQL password:")

if __name__ == "__main__":
    sys.exit(main())
