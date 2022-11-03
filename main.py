"""
FCKC Plane Tracker

Author: Jordan Becquer
7/21/2022
"""

import sys
from math import radians, cos, sin, asin, sqrt
import geopandas
import mysql.connector
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from shapely.geometry import Point, LineString
import geopandas as gpd
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
import logging
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from time import sleep
from datetime import datetime
from threading import Thread
import contextily as ctx


# create logger (copied from https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial)
# logging.basicConfig(filename="logname.txt",
#                     filemode="w+",
#                     format="%(levelname)s - %(message)s",
#                     level=logging.DEBUG)

logger = logging.getLogger('Main')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter('%(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

# Global variables used by function unkw_airport_finder.
origin_fixed = "UNKW"
destination_fixed = "UNKW"


def mysql_connect(database):
    """
    Connect to MySQL server, and grab database using aircraft ID
    :param database: Name of the database to be accessed
    :type database: str
    :return: mysql.connector.connect() is pass, Exception if fail
    """
    try:
        # Init connection to MySQL database
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd=pw,
            database=database
        )
        logger.debug(f" Database connection to {database} successful.")
        return db
    except Exception as e:
        logger.critical(f" {database} database connection failed! (mysql_connect)")
        sys.exit(e)


def between_parentheses(s):
    """
    Take in a string and return what is in-between the parentheses.  # TODO CREATE ERROR CONDITIONS
    :param s: string containing closed parentheses.
    :return: string contained between the parentheses.
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


def check_date(aircraft, check_date):
    """
    Compare the dates between date_last_ran in MySQL and the history grabbed from flight aware
    :return: Returns FALSE if date is older than date_last_ran, TRUE if date is sooner than date_last_ran
    """

    # Establish connection with MySQL and init cursor
    db = mysql_connect("date_last_ran")
    mycursor = db.cursor()

    # Check if the date of this flight occurred before date_last_ran.
    # If true, skip this flight and continue. Else continue with the code
    # First, convert the above string "date" into a datetime date
    date_conv = datetime.strptime(check_date, "%Y-%m-%d")
    date_conv = date_conv.date()  # convert datetime.datetime into datetime.date

    mycursor.execute(f"SELECT date FROM fleet WHERE aircraft = \"{aircraft}\"")

    # fetch the data from the cursor and extract the 0th value
    last_date = mycursor.fetchone()
    last_date = last_date[0]

    if date_conv < last_date:
        # We want all dates INCLUDING the same date, in case flights happened later in the day following the last check
        return True
    else:
        return False


def date_last_ran(tail_num):
    """
    Save the date that the aircraft last successfully ran and saved. This date will be referenced by future runs.
    """

    # Establish connection with MySQL and init cursor
    db = mysql_connect("date_last_ran")
    mycursor = db.cursor()

    # # Create base table
    # mycursor.execute("CREATE TABLE IF NOT EXISTS date_last_ran.fleet("
    #                  "aircraft VARCHAR(10), "
    #                  "date DATE")

    # get the current date using datetime, convert to string
    curr_date = datetime.today().strftime("%Y-%m-%d")

    try:
        mycursor.execute(f"UPDATE date_last_ran.fleet "
                         f"SET date = \"{curr_date}\" "
                         f"WHERE aircraft = \"{tail_num}\"")
        # commit the update to the database
        db.commit()
    except Exception as e:
        logger.warning(f" Error while attempting to update the date_last_ran")
        logger.warning(e)
    else:
        logger.debug(f" Date last ran updated successfully!")


def unkw_airport_finder(url, orig_flag=False):
    """
    Determine which airport is the origin/destination airport. Used to handle situations where flightaware gives
    any response other than the airport identifier (e.g. lat/long coordinates or other misc. code)
    If origin airport, set TRUE
    :rtype: str
    :return: ICAO airport identifier code. "UNKW" if still unable to determine
    """
    # We could try different methods here... most simple would be to open the URL and have the user determine the
    # airport identifier code manually. Least likely to introduce errors or unneeded complexity
    # TODO UPDATE FUNCTION TO ELABORATE IF IT IS ORIGIN OR ARRIVAL
    # Establish new TKinter window
    finder = tk.Tk()
    finder.title("UNKW Airport Finder")
    finder.geometry('390x150+400+400')
    finder.resizable(False, False)

    # Bring the window to the top of the screen
    finder.attributes('-topmost', True)
    finder.update()
    finder.attributes('-topmost', False)

    # SPACER
    spacer1 = tk.Label(finder, text="")
    spacer1.grid(
        row=0,
        column=0)

    # LABEL: url copy
    url_label = ttk.Label(finder, text="Copy the below URL:")
    url_label.grid(
        column=1,
        row=1,
        columnspan=4,
        padx=25,
        sticky="SW")

    # create textbox that contains the flightaware URL
    # TEXT: URL output
    url_output = tk.Text(finder, height=1, width=25)
    url_output.grid(
        column=1,
        row=2,
        columnspan=4,
        padx=25,
        sticky="W")
    url_output.insert(tk.END, "https://flightaware.com" + str(url))
    # Disable editing of the text.
    url_output.configure(state="disabled")

    # LABEL: instruction step 2
    inst_two = ttk.Label(finder, text="Determine the ICAO code of the airport and enter below:")
    inst_two.grid(
        column=1,
        row=3,
        columnspan=4,
        padx=25,
        sticky="W")

    # create second textbox that will allow user to enter airport code
    # TEXT: ICAO input
    code_input = tk.Text(finder, height=1, width=25)
    code_input.grid(
        column=1,
        row=4,
        columnspan=4,
        padx=25,
        sticky="W")

    # BUTTON: Done
    done_button = ttk.Button(
        finder,
        text="Done",
        command=lambda: done_button())
    done_button.grid(
        column=2,
        row=5,
        pady=5)

    # BUTTON: Skip
    skip_button = ttk.Button(
        finder,
        text="Skip",
        command=lambda: skip_button())
    skip_button.grid(
        column=3,
        row=5)

    # cancel button
    # BUTTON: Cancel
    cancel_button = ttk.Button(
        finder,
        text="Cancel",
        command=lambda: cancel_button())
    cancel_button.grid(
        column=4,
        row=5)

    def done_button():
        """
        Confirm whether the entered airport code was correct. Buttons to select confirm or cancel.
        If confirm: return ICAO code as string
        If cancel: destroy confirm window
        :return:
        """
        airport_code = code_input.get("1.0", "end")

        # Catch no entry errors
        if len(airport_code) == 1:
            error_no_entry()
        else:

            # Establish new TKinter window
            done_win = tk.Toplevel()
            done_win.title("Confirm")
            done_win.geometry('250x130+450+425')
            done_win.resizable(False, False)

            # SPACER
            done_spacer = tk.Label(done_win, text="")
            done_spacer.grid(
                row=0,
                column=0)

            # LABEL: url copy
            done_code = ttk.Label(done_win, text="Confirm this is the correct airport code: ")
            done_code.grid(
                column=1,
                row=1,
                columnspan=3,
                pady=5)

            # LABEL: ICAO code
            done_code = ttk.Label(done_win, text=airport_code.upper(), font=("Helvetica", 12, "bold"))
            done_code.grid(
                column=1,
                row=2,
                columnspan=3)

            # BUTTON: Confirm button
            confirm_but = ttk.Button(
                done_win,
                text="Confirm",
                command=lambda: confirm_code(airport_code))
            confirm_but.grid(
                column=1,
                row=3,
                padx=10)

            # BUTTON: Try again button
            try_but = ttk.Button(
                done_win,
                text="Try again",
                command=done_win.destroy)
            try_but.grid(
                column=2,
                row=3)

            def confirm_code(code_in):
                """
                Confirmation that the ICAO code is correct, return new airport code to main.
                Close confirm window and finder window
                :return: ICAO code
                :rtype: str
                """
                if orig_flag:
                    global origin_fixed
                    origin_fixed = code_in
                else:
                    global destination_fixed
                    destination_fixed = code_in
                done_win.destroy()
                finder.destroy()
                finder.quit()

    def skip_button():  # TODO CHANGE THIS TO ACTUALLY SKIP, INSTEAD OF RETURNING UNKW?
        # Establish new TKinter window
        skip_win = tk.Toplevel()
        skip_win.title("Confirm")
        skip_win.geometry('325x100+450+425')
        skip_win.resizable(False, False)

        # SPACER
        skip_spacer = tk.Label(skip_win, text="")
        skip_spacer.grid(
            row=0,
            column=0)

        # LABEL:
        skip_code = ttk.Label(skip_win, text="Confirm you wish to skip this entry. UNKW will be used. ")
        skip_code.grid(
            column=1,
            row=1,
            columnspan=3,
            pady=5)

        # BUTTON: Confirm
        confirm_but = ttk.Button(
            skip_win,
            text="Confirm",
            command=lambda: confirm_code())
        confirm_but.grid(
            column=1,
            row=3,
            padx=10)

        # BUTTON: No
        no_but = ttk.Button(
            skip_win,
            text="No",
            command=skip_win.destroy)
        no_but.grid(
            column=2,
            row=3)

        def confirm_code():
            """
            Used when an airport code cannot be determined or simply wishing to skip.
            :return: UNKW
            :rtype: str
            """
            global origin_fixed
            origin_fixed = "UNKW"
            skip_win.destroy()
            finder.destroy()
            finder.quit()

    def cancel_button():
        """
        Bailout button. Will completely exit the code.
        """
        # Establish new TKinter window
        cancel_win = tk.Toplevel()
        cancel_win.title("Confirm")
        cancel_win.geometry('200x100+450+425')
        cancel_win.resizable(False, False)

        # SPACER
        cancel_spacer = tk.Label(cancel_win, text="")
        cancel_spacer.grid(
            row=0,
            column=0)

        # LABEL:
        cancel_text = ttk.Label(cancel_win, text="Confirm you wish to cancel.")
        cancel_text.grid(
            column=1,
            row=1,
            columnspan=3,
            pady=2)

        # LABEL:
        cancel_text2 = ttk.Label(cancel_win, text="This will completely exit the code.")
        cancel_text2.grid(
            column=1,
            row=2,
            columnspan=3,
            pady=2)

        # BUTTON: Confirm
        confirm_but = ttk.Button(
            cancel_win,
            text="Confirm",
            command=lambda: cancel_all())
        confirm_but.grid(
            column=1,
            row=4,
            padx=10)

        # BUTTON: No
        no_but = ttk.Button(
            cancel_win,
            text="No",
            command=cancel_win.destroy)
        no_but.grid(
            column=2,
            row=4)

        def cancel_all():
            """
            Exit the entire code!
            """
            cancel_win.destroy()
            finder.destroy()
            sys.exit()

    def error_no_entry():
        # create message box that contains the error if no aircraft were selected
        none_select = tk.Toplevel(finder)
        none_select.title("Error!")
        none_select.resizable(False, False)

        # Position message box to be coordinated with the root window
        root_x = finder.winfo_rootx()
        root_y = finder.winfo_rooty()
        win_x = root_x + 100
        win_y = root_y + 25
        none_select.geometry(f'+{win_x}+{win_y}')

        # create the label on the message box
        prog_msg = tk.Label(none_select, text=f" Error, no airport code entered.")
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

    # Execute the window
    Thread(target=finder.mainloop()).start()


def flightaware_history(aircraft):
    """
    Grab the aircraft history from flight aware and return pandas dataframe containing history data.
    :param aircraft: aircraft ID. ex: N182WK
    :type aircraft: str
    :return: pandas df = [date, route, dept_time, time_aloft, URL]
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
        df = pd.DataFrame(columns=["date", "route", "dept_time", "time_aloft", "url"])

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

            # Convert strings into a format that will allow them to be used as table names
            date = convert_date(date)

            # check if the date of the flight is before or after our date_last_ran
            if check_date(aircraft, date):
                logger.debug("Skipping flight that has already been logged...")
                continue
            else:
                pass

            try:
                # If the airport is unknown it is listed as "Near" and no airport code given.
                # unkw_airport_finder allows to modify the global variable and get the correct airport code
                if "Near" in columns[2].text:
                    Thread(target=unkw_airport_finder(url, orig_flag=True)).start()
                    origin = origin_fixed.upper().strip()
                else:
                    origin = between_parentheses(columns[2].text)
                if "Near" in columns[3].text:
                    Thread(target=unkw_airport_finder(url, orig_flag=False)).start()
                    destination = destination_fixed.upper().strip()
                else:
                    destination = between_parentheses(columns[3].text)
                route = origin + "-" + destination

                # # Reset the global variables for the next run to avoid any potential runaway errors with incorrect codes
                # global origin_fixed, destination_fixed
                # origin_fixed = "UNKW"
                # destination_fixed = "UNKW"

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

            aloft = columns[6].text.strip()

            route = route.replace("-", "_")
            dept_time = dept_time.replace(":", "_")
            # build a row to be exported to pandas
            out = [date, route, dept_time[:-3:], aloft, url]
            # build pandas
            df.loc[len(df)] = out
        logger.info(f" {aircraft} history saved successfully!")
        return df

    except Exception as e:
        logger.critical(f" Failed to extract flight history! (flightaware_history)")
        logger.critical(f" error: {e}")
        logger.critical(f" Attempting to skip this row: {row}")


def flightaware_getter(url):
    """
    Web scraping to grab track data from flight aware and save to pandas dataframe
    :param url: The url extracted from MySQL flight_history table, EXCLUDING flightaware.com and /track
    example: https://flightaware.com/live/flight/N81673/history/20220715/1927Z/KLXT/KAMW/tracklog
    should be given as: live/flight/N81673/history/20220715/1927Z/KLXT/KAMW
    :return: Panda dataframe containing [time, lat, long, kts, altitude]
    """

    # Make a GET request to flightaware
    url = "https://flightaware.com" + f"{url}" + "/tracklog"
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
        logger.critical(f" Error: {e}")
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

    # Get pandas dataframe for plane history [date, route, dept_time, time_aloft, url]
    hist_df = flightaware_history(aircraft)

    # catch edge case in flightaware_history, where no flight data exists from the past 14 days. Func will return None
    if hist_df is None:
        return

    # logger.debug(f" Size of the hist_df dataframe: {hist_df.size}")

    # Establish connection with MySQL and initialize the cursor
    db = mysql_connect(aircraft)
    mycursor = db.cursor()

    # Create flight history parent table
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history("
                     "date DATE, "
                     "route VARCHAR(15), "
                     "dept_time VARCHAR(15), "
                     "time_aloft VARCHAR(6), "
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
    mycursor.execute("CREATE TABLE IF NOT EXISTS flight_history_temp "
                     "SELECT DISTINCT date, route, dept_time, time_aloft, url "
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

        # Update the date last ran in MySQL to be used for future flightaware calls.
        date_last_ran(aircraft)

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
            url_list.append(x[4])
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
            # logger.debug(f" The size of the details_df is: {details_df}")
            if details_df is None:
                logger.critical(f" details_df is empty!")
                continue
            # get the table name using the flightaware_combined_hist,
            # by searching the dictionary with the URL (new_flights[i])
            table_name = [z for z in flightaware_combined_hist if flightaware_combined_hist[z] == new_flights[i]]
            table_name = str(table_name[0])
            # Convert dataframe to sql table (flight details)
            details_df.to_sql(table_name.lower(), engine, if_exists="replace", index=False)
            logger.info(f" {i + 1} out of {len(new_flights)} completed!")
            if i != len(new_flights) - 1:
                logger.info(" Waiting 3 seconds...")
                sleep(3)
        except Exception as e:
            logger.warning(f" An error occurred while trying to populate the flight data tables! (db_data_saver)")
            logger.warning(f" Error: {e}")
            logger.warning(" Waiting 3 seconds...")
            sleep(3)
    logger.info(f" Tables built successfully!")

    # Update the date last ran in MySQL to be used for future flightaware calls.
    date_last_ran(aircraft)

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

    # convert month string format to number (January -> 1)
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

    # convert the month to a number, if not all selected
    if month != "All":
        month = month_dates[month]

    # Use the flight history table
    try:
        if month != "All":
            mycursor.execute(f"SELECT * FROM flight_history "
                             f"WHERE month(date)={month} "
                             f"ORDER BY date ASC")
        else:
            mycursor.execute(f"SELECT * FROM flight_history")
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

    # Defining of the dataframe that will contain all of the flight history data
    total_df = pd.DataFrame()

    # for each piece of history, get the flight data
    # Set the ID equal to a unique index, to allow seperate flights to have their own line segment (ref: full_area_map)
    # If we don't have this, the data is drawn as a single line which causes "jumping" between multiple flights
    # that aren't ordered together exactly
    try:
        i = 1  # init counter
        for leg in hist:
            query = f"SELECT * FROM {leg}"
            res_df = pd.read_sql(query, engine)
            res_df["ID"] = str(i)
            i += 1
            if res_df.empty:
                continue
            total_df = pd.concat([total_df, res_df], ignore_index=True)
    except Exception as e:
        logger.warning(f" Error while grabbing {leg}: {e}")
        logger.warning(f" Attempting to continue...")

    return total_df


def calculate_stats(fleet, month):
    """ Calculate various stats related to the aircraft's history"""

    def dist_travelled(data_df):
        """
        Calculate the total distance travelled by the aircraft using lat/long data.
        :return: Total distance travelled in miles
        :rtype: float(2)
        """

        # TODO dist_travelled needs to be reviewed. Output is not correct at all... showing 10,000 miles travelled
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

        total_dist = 0
        latitude = []
        longitude = []
        for row in data_df.itertuples(index=False):
            latitude.append(row.latitude)
            longitude.append(row.longitude)
        latitude = [float(x) for x in latitude]
        longitude = [float(x) for x in longitude]
        for x in range(len(longitude[:-1:])):
            total_dist += lat_long_dist(latitude[x], latitude[x + 1], longitude[x], longitude[x + 1])
        print(f" The total distance travelled was {round(total_dist, 2)} Miles")
        return total_dist

    def time_aloft(aircraft, month):
        """
        Calculate the max time aloft and average time aloft
        :return: total time aloft, average time aloft
        """

        db = mysql_connect(aircraft)
        mycursor = db.cursor()

        # convert month string format to number (January -> 1)
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

        # convert the month to a number, if not all selected
        if month != "All":
            month = month_dates[month]

        # Use the flight history table
        try:
            if month != "All":
                mycursor.execute(f"SELECT time_aloft FROM {aircraft}.flight_history "
                                 f"WHERE month(date)={month}")
            else:
                mycursor.execute(f"SELECT time_aloft FROM flight_history")

        except Exception as e:
            db.close()
            logger.critical(" An error occurred while grabbing the time aloft! (time_aloft)")
            logger.critical(e)
            sys.exit(e)

        # build list of aloft time
        aloft = []
        for x in mycursor:
            # Filter out any potential errors, or old history from the database that may contain a 0 flight time
            if x[0].strip("'") != "0":
                aloft.append(x[0].strip("'"))

        # crunch the data (convert from string (##:##) into engine hours (HH.MM)
        hours_list = []
        minutes_list = []
        for x in aloft:
            hours = int(x.split(":")[0])
            minutes = int(x.split(":")[1])

            hours_list.append(hours)
            minutes_list.append(minutes)

        # Find the sum of hours and minutes, add them together, and convert into Hobbs time
        time_aloft = sum(hours_list) + round(sum(minutes_list) / 60, 1)

        # Calculate average time aloft
        hour_avg = sum(hours_list) / len(hours_list)
        min_avg = round(sum(minutes_list) / len(minutes_list) / 60, 1)

        avg_aloft = round(hour_avg + min_avg, 1)
        print(f" The total time aloft was {time_aloft}")
        print(f" The average time aloft was {avg_aloft}")

        return time_aloft, avg_aloft

    def airports_visited(aircraft, month):
        """Determine the airports visited"""
        # Establish connection with MySQL and init cursor
        db = mysql_connect(aircraft)
        mycursor = db.cursor()

        # convert month string format to number (January -> 1)
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

        # convert the month to a number, if not all selected
        if month != "All":
            month = month_dates[month]

        try:
            if month != "All":
                mycursor.execute(f"SELECT * FROM flight_history "
                                 f"WHERE month(date)={month}")
            else:
                mycursor.execute(f"SELECT * FROM flight_history")
            hist = []
            for x in mycursor:
                # extract the route information, using the destination as the airport used for graphing/stats
                # TODO THIS COULD BE UPDATED TO SELECT ROUTE FROM FLIGHT HISTORY, INSTEAD OF SELECT *
                dest = x[1].split("_")[1]
                hist.append(dest)
        except Exception as e:
            db.close()
            logger.critical(" An error occurred while getting the airports used! (airports_visited)")
            logger.critical(e)
            sys.exit(e)

        # exit condition if no flight history
        if not hist or len(hist) == 0:
            logger.info(f" Possible error condition (airports_visited)")
            return

        # Find the number of unique airports and save to dictionary, count each time the airport occurs
        landing_hist = {}
        for airport in hist:
            if airport not in landing_hist:
                landing_hist[airport] = 1
            else:
                landing_hist[airport] += 1

        # Remove UNKW from dictionary
        if "UNKW" in landing_hist:
            landing_hist.pop("UNKW")

        # sort the airports list
        landing_hist = dict(sorted(landing_hist.items(), key=lambda item: item[1], reverse=True))
        print(f" Trips to the following airports:")
        print(landing_hist)

    print(f" ~~~~~~~~~~~~~~~~~ {month} stat line-up ~~~~~~~~~~~~~~~~~")

    # N81673 Archer
    if "N81673" in fleet:
        df_N81673 = db_data_getter("N81673", month)
        # Catch condition where there are is no flight history
        if not df_N81673.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N81673 (Archer) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N81673)
            time_aloft("N81673", month)
            airports_visited("N81673", month)

    # N3892Q C172 (OJC)
    if "N3892Q" in fleet:
        df_N3892Q = db_data_getter("N3892Q", month)
        # Catch condition where there are is no flight history
        if not df_N3892Q.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N3892Q (C172) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N3892Q)
            time_aloft("N3892Q", month)
            airports_visited("N3892Q", month)

    # N20389 C172 (OJC)
    if "N20389" in fleet:
        df_N20389 = db_data_getter("N20389", month)
        # Catch condition where there are is no flight history
        if not df_N20389.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N20389 (C172) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N20389)
            time_aloft("N20389", month)
            airports_visited("N20389", month)

    # N182WK C182 (LXT)
    if "N182WK" in fleet:
        df_N182WK = db_data_getter("N182WK", month)
        # Catch condition where there are is no flight history
        if not df_N182WK.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N182WK (C182) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N182WK)
            time_aloft("N182WK", month)
            airports_visited("N182WK", month)

    # N58843 C182 (OJC)
    if "N58843" in fleet:
        df_N58843 = db_data_getter("N58843", month)
        # Catch condition where there are is no flight history
        if not df_N58843.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N58843 (C182) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N58843)
            time_aloft("N58843", month)
            airports_visited("N58843", month)

    # N82145 Saratoga
    if "N82145" in fleet:
        df_N82145 = db_data_getter("N82145", month)
        # Catch condition where there are is no flight history
        if not df_N82145.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N82145 (Saratoga) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N82145)
            time_aloft("N82145", month)
            airports_visited("N82145", month)

    # N4803P Debonair
    if "N4803P" in fleet:
        df_N4803P = db_data_getter("N4803P", month)
        # Catch condition where there are is no flight history
        if not df_N4803P.empty:
            print(f" ~~~~~~~~~~~~~~~~~ Stats for N4803P (Debonair) ~~~~~~~~~~~~~~~~~")
            dist_travelled(df_N4803P)
            time_aloft("N4803P", month)
            airports_visited("N4803P", month)


def airport_coordinates(airport):
    """
    Get the airport code from mySQL. If not available from mySQL, scrape airnav.com and save those coordinates to mySQL
    for future use.
    :param airport: ICAO airport code
    :return: set of lat/long coordinates
    """
    # Establish connection with MySQL and init cursor
    db = mysql_connect("airport_coords")
    mycursor = db.cursor()

    # Create SQLAlchemy engine to connect to MySQL Database
    user = "root"
    passwd = pw
    database = "airport_coords"
    host_ip = '127.0.0.1'
    port = "3306"

    engine = create_engine(
        'mysql+mysqlconnector://' + user + ':' + passwd + '@' + host_ip + ':' + port + '/' + database,
        echo=False)

    # Create coordinates table
    mycursor.execute("CREATE TABLE IF NOT EXISTS coords("
                     "latitude FLOAT(9,4), "
                     "longitude FLOAT(9,4), "
                     "airport VARCHAR(15))")

    mycursor.execute(f"SELECT * FROM coords")

    # Get the list of airports that already have coordinates defined in the database
    existing_airport = []
    for x in mycursor:
        stepper = str(x[2])
        existing_airport.append(stepper)

    if airport in existing_airport:
        # if the airport is already in the database, return in format: lat, long, airport code
        mycursor.execute(f"SELECT * FROM coords WHERE airport = \"{airport}\"")
        for results in mycursor:
            return results

    else:
        # else scrape airnav.com to find the lat long data
        # Make a GET request to flightaware
        url = "https://airnav.com/airport/" + f"{airport}"
        logger.info(f" Getting GPS coordinate data from URL: {url}")
        r = requests.get(url, timeout=5)
        # Check the status code
        if r.status_code != 200:
            logger.critical(f" Failed to connect to Airnav.com! (airport_coordinates)")
            logger.critical(f" status code: {r.status_code}")
            sys.exit(r.status_code)

        # Parse the HTML
        soup = BeautifulSoup(r.text, "html.parser")
        # ------------------------------------------------------------------------------------------------------------------
        #   Extract table data
        # ------------------------------------------------------------------------------------------------------------------
        # find the latitude and longitude coordinates provided on airnav.com
        try:
            s = soup.findAll("table")
            raw_coords = s[6]
            rows = raw_coords.find_all("tr")
            column = rows[2].find_all("td")
            column = str(column).split("<br/>")
            column = column[2].split(",")
            lat = column[0]
            long = column[1]
        except Exception as e:
            logger.critical(f" Error finding information on Airnav.com! (airport_coordinates)")
            logger.critical(f" Error: {e}")
            sys.exit()

        # Save the newfound lat/long coordinates in mySQL, first saving it as a pandas DF
        # build a row to be exported to pandas
        res = [lat, long, airport]
        # build pandas
        airport_df = pd.DataFrame([res], columns=["latitude", "longitude", "airport"])

        try:
            # Convert dataframe to sql table (airport_coordinates)
            airport_df.to_sql('coords', engine, if_exists="append", index=False)
        except Exception as e:
            logger.critical(" An error occurred with the SQLAclhemy engine! (airport_coordinates)")
            logger.critical(f" Error: {e}")
            sys.exit(e)

    # finally, return the airport information
    return res


def airports_plotter(aircraft, month):
    """Determine the airports visited specifically to be used for plotting in Geopandas"""
    # Establish connection with MySQL and init cursor
    db = mysql_connect(aircraft)
    mycursor = db.cursor()

    # convert month string format to number (January -> 1)
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

    # convert the month to a number, if not all selected
    if month != "All":
        month = month_dates[month]

    try:
        if month != "All":
            mycursor.execute(f"SELECT * FROM flight_history "
                             f"WHERE month(date)={month}")
        else:
            mycursor.execute(f"SELECT * FROM flight_history")
        hist = []
        for x in mycursor:
            # convert DATE format to string with underscores to allow to be used as table name
            dest = x[1].split("_")[1]
            hist.append(dest)
    except Exception as e:
        db.close()
        logger.critical(" An error occurred while getting the route history! (airports_visited)")
        logger.critical(e)
        sys.exit(e)

    # exit condition if no flight history. Return "UNKW" to avoid trying to concatenate empty lists
    if not hist or len(hist) == 0:
        logger.debug(f" {aircraft} has no flight history!")
        landing_hist_list = ["UNKW"]
        return landing_hist_list

    # Find the number of unique airports and save to dictionary, count each time the airport occurs
    landing_hist = {}
    for airport in hist:
        if airport not in landing_hist:
            landing_hist[airport] = 1
        else:
            landing_hist[airport] += 1

    # sort the airports list
    landing_hist = dict(sorted(landing_hist.items(), key=lambda item: item[1], reverse=True))
    # extract only the keys of the dictionary
    landing_hist_list = list(landing_hist.keys())
    return landing_hist_list


def full_area_map(fleet, month, option, local):
    """Use the lat/long data to plot a composite map of the KC area
    TODO ADD DOCSTRING TO full_area_map
    """

    # Define the map
    if not local:
        ax = plt.subplot()
        # hide the x and y-axis labels
        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
    else:
        KC = ctx.Place("Kansas City", zoom=12)
        # "Loch Lloyd, MO", zoom=9
        ax = KC.plot()
        ax.autoscale(False)

    # N81673 Archer
    if "N81673" in fleet:
        df_N81673 = db_data_getter("N81673", month)
        airports_N81673 = airports_plotter("N81673", month)
        # Catch condition where there are is no flight history
        if not df_N81673.empty:
            geom_N81673 = \
                [Point(xy) for xy in zip(df_N81673["longitude"].astype(float), df_N81673["latitude"].astype(float))]
            gdf_N81673 = GeoDataFrame(df_N81673, geometry=geom_N81673)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N81673.crs = "EPSG:4326"
            gdf_N81673 = gdf_N81673.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N81673_line = gdf_N81673.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N81673_line = gpd.GeoDataFrame(gdf_N81673_line, geometry="geometry")
                gdf_N81673_line.plot(ax=ax, color="red", markersize=1, label="Archer - N81673")
            else:
                gdf_N81673 = gpd.GeoDataFrame(gdf_N81673, geometry="geometry")
                gdf_N81673.plot(ax=ax, color="red", markersize=1, label="Archer - N81673")
    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N81673 = []

    # N3892Q C172 (OJC)
    if "N3892Q" in fleet:
        df_N3892Q = db_data_getter("N3892Q", month)
        airports_N3892Q = airports_plotter("N3892Q", month)
        # Catch condition where there are is no flight history
        if not df_N3892Q.empty:
            geom_N3892Q = \
                [Point(xy) for xy in zip(df_N3892Q["longitude"].astype(float), df_N3892Q["latitude"].astype(float))]
            gdf_N3892Q = GeoDataFrame(df_N3892Q, geometry=geom_N3892Q)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N3892Q.crs = "EPSG:4326"
            gdf_N3892Q = gdf_N3892Q.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N3892Q_line = gdf_N3892Q.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N3892Q_line = gpd.GeoDataFrame(gdf_N3892Q_line, geometry="geometry")
                gdf_N3892Q_line.plot(ax=ax, color="blue", markersize=1, label="C172 - N3892Q")
            else:
                gdf_N3892Q = gpd.GeoDataFrame(gdf_N3892Q, geometry="geometry")
                gdf_N3892Q.plot(ax=ax, color="blue", markersize=1, label="C172 - N3892Q")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N3892Q = []

    # N20389 C172 (OJC)
    if "N20389" in fleet:
        df_N20389 = db_data_getter("N20389", month)
        airports_N20389 = airports_plotter("N20389", month)
        # Catch condition where there are is no flight history
        if not df_N20389.empty:
            geom_N20389 = \
                [Point(xy) for xy in zip(df_N20389["longitude"].astype(float), df_N20389["latitude"].astype(float))]
            gdf_N20389 = GeoDataFrame(df_N20389, geometry=geom_N20389)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N20389.crs = "EPSG:4326"
            gdf_N20389 = gdf_N20389.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N20389_line = gdf_N20389.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N20389_line = gpd.GeoDataFrame(gdf_N20389_line, geometry="geometry")
                gdf_N20389_line.plot(ax=ax, color="green", markersize=1, label="C172 - N20389")
            else:
                gdf_N20389 = gpd.GeoDataFrame(gdf_N20389, geometry="geometry")
                gdf_N20389.plot(ax=ax, color="green", markersize=1, label="C172 - N20389")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N20389 = []

    # N182WK C182 (LXT)
    if "N182WK" in fleet:
        df_N182WK = db_data_getter("N182WK", month)
        airports_N182WK = airports_plotter("N182WK", month)
        # Catch condition where there are is no flight history
        if not df_N182WK.empty:
            geom_N182WK = \
                [Point(xy) for xy in zip(df_N182WK["longitude"].astype(float), df_N182WK["latitude"].astype(float))]
            gdf_N182WK = GeoDataFrame(df_N182WK, geometry=geom_N182WK)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N182WK.crs = "EPSG:4326"
            gdf_N182WK = gdf_N182WK.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N182WK_line = gdf_N182WK.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N182WK_line = gpd.GeoDataFrame(gdf_N182WK_line, geometry="geometry")
                gdf_N182WK_line.plot(ax=ax, color="orange", markersize=1, label="C182 - N182WK")
            else:
                gdf_N182WK = gpd.GeoDataFrame(gdf_N182WK, geometry="geometry")
                gdf_N182WK.plot(ax=ax, color="orange", markersize=1, label="C182 - N182WK")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N182WK = []

    # N58843 C182 (LXT)
    if "N58843" in fleet:
        df_N58843 = db_data_getter("N58843", month)
        airports_N58843 = airports_plotter("N58843", month)
        # Catch condition where there are is no flight history
        if not df_N58843.empty:
            geom_N58843 = \
                [Point(xy) for xy in zip(df_N58843["longitude"].astype(float), df_N58843["latitude"].astype(float))]
            gdf_N58843 = GeoDataFrame(df_N58843, geometry=geom_N58843)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N58843.crs = "EPSG:4326"
            gdf_N58843 = gdf_N58843.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N58843_line = gdf_N58843.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N58843_line = gpd.GeoDataFrame(gdf_N58843_line, geometry="geometry")
                gdf_N58843_line.plot(ax=ax, color="grey", markersize=1, label="C182 - N58843")
            else:
                gdf_N58843 = gpd.GeoDataFrame(gdf_N58843, geometry="geometry")
                gdf_N58843.plot(ax=ax, color="grey", markersize=1, label="C182 - N58843")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N58843 = []

    # N82145 Saratoga
    if "N82145" in fleet:
        df_N82145 = db_data_getter("N82145", month)
        airports_N82145 = airports_plotter("N82145", month)
        # Catch condition where there are is no flight history
        if not df_N82145.empty:
            geom_N82145 = \
                [Point(xy) for xy in zip(df_N82145["longitude"].astype(float), df_N82145["latitude"].astype(float))]
            gdf_N82145 = GeoDataFrame(df_N82145, geometry=geom_N82145)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N82145.crs = "EPSG:4326"
            gdf_N82145 = gdf_N82145.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N82145_line = gdf_N82145.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N82145_line = gpd.GeoDataFrame(gdf_N82145_line, geometry="geometry")
                gdf_N82145_line.plot(ax=ax, color="black", markersize=1, label="Saratoga - N82145")
            else:
                gdf_N82145 = gpd.GeoDataFrame(gdf_N82145, geometry="geometry")
                gdf_N82145.plot(ax=ax, color="black", markersize=1, label="Saratoga - N82145")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N82145 = []

    # N4803P Debonair
    if "N4803P" in fleet:
        df_N4803P = db_data_getter("N4803P", month)
        airports_N4803P = airports_plotter("N4803P", month)
        # Catch condition where there are is no flight history
        if not df_N4803P.empty:
            geom_N4803P = \
                [Point(xy) for xy in zip(df_N4803P["longitude"].astype(float), df_N4803P["latitude"].astype(float))]
            gdf_N4803P = GeoDataFrame(df_N4803P, geometry=geom_N4803P)

            # define the coordinates initially as 4326 then convert to 3857
            gdf_N4803P.crs = "EPSG:4326"
            gdf_N4803P = gdf_N4803P.to_crs(epsg=3857)

            if option == "Lines":
                gdf_N4803P_line = gdf_N4803P.groupby(["ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
                gdf_N4803P_line = gpd.GeoDataFrame(gdf_N4803P_line, geometry="geometry")
                gdf_N4803P_line.plot(ax=ax, color="magenta", markersize=1, label="Debonair - N4803P")
            else:
                gdf_N4803P = gpd.GeoDataFrame(gdf_N4803P, geometry="geometry")
                gdf_N4803P.plot(ax=ax, color="magenta", markersize=1, label="Debonair - N4803P")

    else:
        # Create an empty list to allow for the airports plotter to combine all lists correctly
        airports_N4803P = []

    # Combined all the airport data, save only the unique values
    airports_fleet = list(set(airports_N81673 +
                              airports_N3892Q +
                              airports_N20389 +
                              airports_N182WK +
                              airports_N58843 +
                              airports_N82145 +
                              airports_N4803P))
    # Remove UNKW from list
    # This catches both UNKW airports as well as aircraft that returned "UNKW" because the flight history was empty
    if "UNKW" in airports_fleet:
        airports_fleet.remove("UNKW")

    # create list of visited airports in a list with format [lat, long, airport code]
    airport_coords = []
    for airport in airports_fleet:
        airport_coords.append(airport_coordinates(airport))

    coord_df = pd.DataFrame(columns=["latitude", "longitude", "airport"])

    for step in airport_coords:
        coord_df.loc[len(coord_df)] = step

    # create geodataframe of airports
    airport_gdf = \
        geopandas.GeoDataFrame(coord_df, geometry=geopandas.points_from_xy(coord_df.longitude, coord_df.latitude))
    airport_gdf = airport_gdf.set_crs(epsg=4326)
    airport_gdf = airport_gdf.to_crs(epsg=3857)
    for x, y, label in zip(airport_gdf.geometry.x, airport_gdf.geometry.y, airport_gdf.airport):
        ax.annotate(label, xy=(x, y), xytext=(3, 3), textcoords="offset points", )

    # finally, plot
    plt.legend(loc="upper left")
    if month != "All":
        plt.title(f"{month} flight history")
    else:
        plt.title(f"2022 flight history")

    if not local:
        ctx.add_basemap(ax)

    plt.show()
    pass


def main():
    """Main entry point for the script."""

    # -------------------------------------------------------------------------------------------------------------------
    #                                                   TKINTER GUI WINDOW
    #                                       Reference https://www.pythontutorial.net/tkinter
    # -------------------------------------------------------------------------------------------------------------------

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
             "N3892Q - C172",
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
                logger.critical(f" Error: {e}")
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

        # # create message box that contains a progress bar on the status of the fleet
        # aircraft_progress = tk.Toplevel(root)
        # aircraft_progress.title("Data Gathering Progress")
        # aircraft_progress.resizable(False, False)
        #
        # # Bring the window to the top of the screen
        # aircraft_progress.attributes('-topmost', True)
        # aircraft_progress.update()
        # aircraft_progress.attributes('-topmost', False)
        #
        # # Position message box to be coordinated with the root window
        # root_x = root.winfo_rootx()
        # root_y = root.winfo_rooty()
        # win_x = root_x + 250
        # win_y = root_y + 50
        # aircraft_progress.geometry(f'+{win_x}+{win_y}')
        #
        # # Configure columns/rows
        # aircraft_progress.columnconfigure(1, weight=1)
        # aircraft_progress.rowconfigure(1, weight=1)
        #
        # # create the label on the message box
        # prog_msg = tk.Label(aircraft_progress,
        #                     text=f" Getting aircraft data for: \n{selected_aircraft_str}")
        # prog_msg.grid(column=1, row=0)
        #
        # # create the progressbar
        # pb = ttk.Progressbar(
        #     aircraft_progress,
        #     orient='horizontal',
        #     mode='indeterminate',
        #     length=280)
        #
        # # place the progressbar
        # pb.grid(column=1, row=1, columnspan=2, padx=10, pady=20)
        # pb.start()
        #
        # # BUTTON: cancel data gathering
        # data_cancel_button = ttk.Button(
        #     aircraft_progress,
        #     text="Cancel",
        #     command=lambda: data_cancel())
        # data_cancel_button.grid(
        #     column=1,
        #     row=2)

        # TODO THREADING
        # Call data gathering
        for aircraft in selected_aircraft:
            logger.info(f" ~~~~~~~~~~~~~ {aircraft} ~~~~~~~~~~~~~")
            Thread(target=db_data_saver(aircraft)).start()
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

        # def data_cancel():
        #     log_output.configure(state="normal")  # allow editing of the log
        #     log_output.insert(tk.END, f"Data gathering has been cancelled!\n\n")
        #     aircraft_progress.destroy()
        #     # Always scroll to the index: "end"
        #     log_output.see(tk.END)
        #     log_output.configure(state="disabled")  # disable editing of the log

    def graph_aircraft(map_size):
        """
        TODO ADD DOCSTRING
        :param map_size: "full" or "KC"
        """
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

        # get the current month from the month combobox
        sel_month = month_cb.get()

        # get the plotting option (points or strings) from the sel_options radio buttons
        sel_option = selected_option.get()

        # call the grapher
        if map_size == "full":
            full_area_map(sel_aircraft, sel_month, sel_option, False)
        else:
            full_area_map(sel_aircraft, sel_month, sel_option, True)
            

        # log the commands
        log_output.configure(state="normal")  # allow editing of the log
        log_output.insert(tk.END,
                          f" \nA local graph with the following aircraft has been created:\n {sel_aircraft_str}")
        log_output.insert(tk.END, f"\n\n")
        # Always scroll to the index: "end"
        log_output.see(tk.END)
        log_output.configure(state="disabled")  # disable editing of the log

    def calculate_stats_tkinter():
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

        # get the current month from the month combobox
        sel_month = month_cb.get()

        calculate_stats(sel_aircraft, sel_month)

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
            mycursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}("
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

    # define the row where the main buttons are
    bot_button_row = 4

    # COMBOBOX: Select month
    selected_month = tk.StringVar()
    month_cb = ttk.Combobox(root, textvariable=selected_month)
    # prevent typing a value
    month_cb["state"] = "readonly"
    # set values
    month_cb["values"] = ["All", "January", "February", "March", "April", "May", "June", "July", "August", "September",
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

    def thread_sub1():  # TODO DOCUMENTATION OF THREAD_SUB1
        Thread(target=get_aircraft_data()).start()

    # BUTTON: Get flight history
    aircraft_button = ttk.Button(
        root,
        text="Get flight history",
        command=lambda: thread_sub1())
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
        command=lambda: calculate_stats_tkinter())
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

    # BUTTON: Create graph (not local)
    aircraft_button = ttk.Button(
        root,
        text="Create graph",
        command=lambda: graph_aircraft("full"))
    aircraft_button.grid(
        column=1,
        row=6,
        sticky="E",
        pady=15)

    # BUTTON: Create local KC graph
    aircraft_button = ttk.Button(
        root,
        text="Create KC graph",
        command=lambda: graph_aircraft("KC"))
    aircraft_button.grid(
        column=2,
        row=6,
        sticky="E",
        pady=15)

    # RADIO BUTTON: Select between points and lines (graphing)
    selected_option = tk.StringVar()
    selected_option.set("Lines")  # Default radio button option
    options = (("Points", "Points"),
               ("Lines", "Lines"))

    for i, option in enumerate(options):
        rad_opt = ttk.Radiobutton(
            root,
            text=option[0],
            value=option[1],
            variable=selected_option,
        )

        rad_opt.grid(
            column=1,
            row=7 + i
        )

    # Execute
    Thread(target=root.mainloop()).start()

    logger.info(" Code complete.")


if __name__ == "__main__":
    sys.exit(main())
