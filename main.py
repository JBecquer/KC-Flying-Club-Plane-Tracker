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
        print(f" Database connection to {aircraft} successful.")
        return db
    except Exception as x:
        print(f" Database connection failed!")
        sys.exit(x)

        
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
    :return: pandas df = [date, route, URL]
    """
    # Make a GET request to flightaware
    url = f"https://flightaware.com/live/flight/{aircraft}/history"
    r = requests.get(url)
    # Check the status code
    if r.status_code != 200:
        print(f" Failed to connect to FlightAware!")
        print(f" status code: {r.status_code}")
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
            print(f" Error finding aircraft history table on FlightAware!")
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
                print(f" Something went wrong while getting the plane history: {e}")
                print(" Attempting to continue...")
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
        return df

    except Exception as e:
        print(f" Failed to extract flight history! (flightaware_history)")
        print(f" error: {e}")
        sys.exit()


def flightaware_getter(url):
    """
    Web scraping to grab data from flight aware and save to file.
    :return: Panda dataframe containing list[time, lat, long, kts, altitude]
    """

    # Make a GET request to flightaware
    url = "https://flightaware.com"+f"{url}"+"/tracklog"
    print(url)
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
    try:
        head = soup.find("head")
        title = head.find("title").text
        # Extracting from: Flight Track Log ✈ N81673 22-Jul-2022 (MO3-KOJC) - FlightAware
        # leg information is between the parenthesis, below code extracts and saves as separate objects
        between_parentheses(title)
        splitter = title.split("-")
        dep_airport = splitter[0]
        dest_airport = splitter[1]
        # TODO ADD RETURN
    except Exception as e:
        print(f" Failed to extract departure and destination airports!")
        print(f" error: {e}")
        sys.exit()
    # ------------------------------------------------------------------------------------------------------------------
    #   Extract table data
    # ------------------------------------------------------------------------------------------------------------------
    # Look for table "prettyTable fullWidth"
    try:
        table = soup.find("table", class_="prettyTable fullWidth")
        if table is None:
            raise Exception(f" Table class not found! {url}")
    except Exception as e:
        print(f" Error finding table on FlightAware!")
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
            builder = [time, latitude, longitude, kts, altitude]

        # Sometimes an empty list is generated due to scraping, reject these.
        if len(builder) == 5:
            df.loc[len(df)] = builder
    return df


def db_data_saver(fleet):
    """
    Export the web scrapped panda dataframe into MySQL
    :param fleet: list of club aircraft
    """

    # Get pandas dataframe for PLANE HISTORY
    hist_df = flightaware_history(fleet[0])

    # Establish connection with MySQL and initialize the cursor
    db = mysql_connect(fleet[0])
    mycursor = db.cursor()

    # Delete a table
    # USED ONLY DURING TESTING TO AVOID BUILD-UP OF DATA  # TODO DELETE
    # mycursor.execute("DROP TABLE flight_history")

    # Delete a table
    # mycursor.execute("DROP TABLE flight")

    # Create flight history PARENT table
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history("
                     "date VARCHAR(15), "
                     "route VARCHAR(15), "
                     "dept_time VARCHAR(15), "
                     "url VARCHAR(100))")

    # Create SQLAlchemy engine to connect to MySQL Database
    user = "root"
    passwd = pw
    database = "N81673"
    host_ip = '127.0.0.1'
    port = "3306"

    engine = create_engine(
        'mysql+mysqlconnector://' + user + ':' + passwd + '@' + host_ip + ':' + port + '/' + database,
        echo=False)

    try:
        # Convert dataframe to sql table (FLIGHT HISTORY)  # TODO FIND WAY TO ONLY REPLACE IF NOT EXISTS
        hist_df.to_sql('flight_history', engine, if_exists="replace", index=False)
    except Exception as e:
        print(" An error occured with the SQLAclhemy engine! (FLIGHT HISTORY)")
        print(f" Error: {e}")
        sys.exit()

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
            print(e)
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
        print(" An error occured with the SQLAclhemy engine! (build url list)")
        print(str(e))
        sys.exit()

    if len(url_list) != len(name):
        sys.exit(f" length of names and length of url_list are not the same!")

    # try to get specific history data from each url page
    for i in range(len(url_list)):
        try:
            details_df = flightaware_getter(url_list[i])
            # Convert dataframe to sql table (flight details)
            details_df.to_sql(name[i].lower(), engine, if_exists="replace", index=False)
            print(f" Apparent success? {i} out of {len(url_list)}")
            print(" Waiting 3 seconds...")
            time.sleep(3)
        except Exception as e:
            print(f" An error occured with the SQLAclhemy engine! (URL BUILDER)")
            print(f" Error: {e}")
            print(" Waiting 3 seconds...")
            time.sleep(3)
    db.close()


def db_data_getter(fleet):
    """
    Import the data from MySQL and convert into pandas dataframe
    :return: pandas dataframe
    """
    # Establish connection with MySQL and init cursor
    db = mysql_connect(fleet[0])
    mycursor = db.cursor()

    # Create SQLAlchemy engine to connect to MySQL Database
    user = "root"
    passwd = pw
    database = "N81673"
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
        print(" An error occured with the SQLAclhemy engine!")
        print(str(e))
        sys.exit()

    # Defining of the dataframe
    total_df = pd.DataFrame()

    try:
        for leg in hist:
            query = f"SELECT * FROM {leg}"
            res_df = pd.read_sql(query, engine)
            if res_df.empty:
                continue
            total_df = pd.concat([total_df, res_df],ignore_index=True)
    except Exception as e:
        print(f" {leg} not found! Attempting to continue...")

    return total_df


def calculate_stats(fleet):
    """ Calculate various stats related to the aircraft's history"""
    # Establish connection with MySQL:
    db = mysql_connect(fleet[0])

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


def local_area_map(fleet):
    """Use the lat/long data to plot a composite map of the KC area"""
    df = db_data_getter(fleet)

    df["longitude"] = df["longitude"].astype(float)
    df["latitude"] = df["latitude"].astype(float)

    # grab the latitude and longitude data from the panda dataframe
    geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
    gdf = GeoDataFrame(df, geometry=geometry)
    ax = state_plotter(["WI", "IA", "MN", "MO", "KS", "NE", "IL"], us_map=False)
    gdf.plot(ax=ax, color="red", markersize=5)
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
        "N182WK",  # C182
        "N58843",  # C182
        "N82145",  # Saratoga
        "N4803P"  # Debonair
    ]
    # flightaware_getter()  # DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    # flightaware_history("N81673")  #  DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    db_data_saver(fleet)
    # db_data_getter(fleet)  # DOES NOT NEED TO BE CALLED HERE, FOR TEST PURPOSES ONLY
    # calculate_stats(fleet)
    local_area_map(fleet)
    pass

# Make pw a global variable so it can be accessed by all the various database calls
pw = getpass(" Enter MySQL password:")


if __name__ == "__main__":
    sys.exit(main())
