from geopy.geocoders import Nominatim

geolocator = Nominatim(
    user_agent="psgc_locator"
)

from getpass import getpass
from mysql.connector import connect, Error
from geopy.exc import GeocoderTimedOut
import re
import time

print("Initializing PSGC Locator using Nominatim...")

def do_geocode(address):
    try:
        time.sleep(1)
        return geolocator.geocode(address)
    except GeocoderTimedOut:
        print("Standing by...");
        time.sleep(10)
        return do_geocode(address)
        

def append_location(connection, code, pLat, pLong):
    if pLat:
        print("Latitude - " + pLat)
    
    if pLong:
        print("Longitude - " + pLong)

    mycursor = connection.cursor()
    sql = "UPDATE `psgc_data` SET latitude = %s, longitude = %s, processed = %s WHERE id = %s || processed = %s ORDER BY id DESC"
    val = (pLat, pLong, 1, code, 0)
    mycursor.execute(sql, val)
    connection.commit()

def process_place(connection, code, barangay, city, province):
    if barangay is None:
        query = city + "," + province;
    else:
        query = barangay + "," + city + "," + province;
    
    
    data = do_geocode(query)
    
    if data is None:
        print(query + ': Not found')
        append_location(connection, code, '', '')
    else:
        lat = data.raw.get("lat")
        long = data.raw.get("lon")
        print(query + ':')
        append_location(connection, code, lat, long)
        
def process_data(connection):
    get_data = "SELECT * FROM psgc_data WHERE latitude > 20.809305 || longitude > 126.5842 || longitude <= 116.9656369 || latitude < 4.6544 || latitude = '' || longitude = '' ORDER BY id DESC"
    with connection.cursor() as cursor:
        cursor.execute(get_data)
        result = cursor.fetchall()
        
        for row in result:
            barangay = re.sub(r" ?\([^)]+\)", "", row[2])
            city = re.sub(r" ?\([^)]+\)", "", row[6])
            province = re.sub(r" ?\([^)]+\)", "", row[10])
            
            process_place(connection, row[0], barangay, city, province)
            
    
try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
        database="psgc_locator",
    ) as connection:
        print("Connection successful... Fetching data from Nominatim")
        
        process_data(connection)
                
except Error as e:
    print(e)