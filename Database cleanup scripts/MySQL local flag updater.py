"""
12/5/2022

Used to update the local flag in MySQL. Change radial_dist and aircraft
as needed.

"""

import sys
import mysql.connector
from math import radians, cos, sin, asin, sqrt
import time

radial_dist = 80
aircraft = "n82145"


# n182wk
# n20389
# n3892q
# n4803p
# n58843
# n81673
# n82145


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

    if lon1 == 0 or lon2 == 0 or lat1 == 0 or lat2 == 0:
        return 0

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


def is_local(lat1, lat2, lon1, lon2, url):
    """
    Determine whether an flight was local. Local is 100 miles of KLXT airport.
    Update the database using the url to find the table row. Flag was defaulted FALSE

    :rtype: bool
    """

    lxt_lat = 38.9577
    lxt_lon = -94.3739

    ojc_lat = 38.9728
    ojc_lon = -94.3732

    orig_dist_lxt = lat_long_dist(lat1, lxt_lat, lon1, lxt_lon)
    dest_dist_lxt = lat_long_dist(lat2, lxt_lat, lon2, lxt_lon)

    orig_dist_ojc = lat_long_dist(lat1, ojc_lat, lon1, ojc_lon)
    dest_dist_ojc = lat_long_dist(lat2, ojc_lat, lon2, ojc_lon)

    print(f"LXT distance: {abs((int(dest_dist_lxt) - int(orig_dist_lxt)))}")
    print(f"OJC distance: {abs((int(dest_dist_ojc) - int(orig_dist_ojc)))}")

    if abs((int(dest_dist_lxt) - int(orig_dist_lxt))) <= radial_dist \
            or abs((int(dest_dist_ojc) - int(orig_dist_ojc))) <= radial_dist:
        # update MySQL local flag to TRUE

        # get the aircraft from the url
        tail_num = url.split("/")[3]

        mycursor3 = db.cursor()
        mycursor3.execute(f"UPDATE {tail_num}.flight_history "
                          f"SET local = true "
                          f"WHERE url = \"{url}\"")
        # commit the update to the database
        db.commit()
        time.sleep(1)
        print(f"{url} has been updated as True!")
    else:
        # get the aircraft from the url
        tail_num = url.split("/")[3]

        mycursor3 = db.cursor()
        mycursor3.execute(f"UPDATE {tail_num}.flight_history "
                          f"SET local = false "
                          f"WHERE url = \"{url}\"")
        # commit the update to the database
        db.commit()
        time.sleep(1)
        print(f"{url} has been updated as FALSE!")


try:
    # Init connection to MySQL database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="v34TpkAEeskT8YTD",
        database=aircraft
    )
except Exception as e:
    sys.exit(e)

mycursor = db.cursor()
mycursor.execute(f"SELECT * from flight_history")
# mycursor.execute(f"SELECT * from flight_history WHERE local is null")

table_name = []
for x in mycursor:
    # convert DATE format into a string with underscores to allow to be used as table name
    date = str(x[0])
    date = date.replace("-", "_")
    hour = x[2]
    hour = hour[0:2:]
    inter = (date + "__" + x[1].lower() + "__" + hour)
    inter2 = [inter, x[5]]
    table_name.insert(0, inter2)
mycursor.close()

# find the latitude and longitude, update flight_history local flag for each row
iter = 1
for z in table_name:
    print("NEW FLIGHT--")
    mycursor = db.cursor()
    mycursor.execute(f"SELECT latitude, longitude "
                     f"FROM {aircraft}.{z[0]} ORDER BY time LIMIT 1;")
    for y in mycursor:
        lat1 = y[0]
        lon1 = y[1]
    mycursor.close()
    time.sleep(1)
    mycursor2 = db.cursor()
    mycursor2.execute(f"SELECT latitude, longitude "
                      f"FROM {aircraft}.{z[0]} ORDER BY time DESC LIMIT 1;")
    for i in mycursor2:
        lat2 = i[0]
        lon2 = i[1]
    if lat1 is None or lat2 is None or lon1 is None or lon2 is None:
        lat1 = 0
        lat2 = 0
        lon1 = 0
        lon2 = 0
        print("Empty database!")

    is_local(float(lat1), float(lat2), float(lon1), float(lon2), z[1])
    mycursor.close()
    print(f"{iter} of {len(table_name)} completed")
    iter += 1
