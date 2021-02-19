from geopy.geocoders import MapBox, Nominatim

#For the api key of mapbox, go to https://account.mapbox.com/, and copy the public api key
geolocator = MapBox(
    api_key= YOUR_API KEY,
    user_agent = "psgc_locator",
    scheme = "https",
    timeout = 10,
    domain='api.mapbox.com')

nom = Nominatim(
    user_agent="psgc_locator"
)

from getpass import getpass
from mysql.connector import connect, Error
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import re
import time

print("Initializing PSGC Locator using Mapbox...")

def do_geocode(address):
    try:
        time.sleep(1)
        return geolocator.geocode(query=address, country = "PH")
    except GeocoderTimedOut:
        print("Standing by...");
        time.sleep(10)
        return do_geocode(address)
    except GeocoderServiceError:
        print("Error encountered... Fetching data from Nominatim")
        return nom.geocode(query=address)

def append_location(connection, code, pLat, pLong):
    if pLat:
        print("Latitude - " + str(pLat))
    
    if pLong:
        print("Longitude - " + str(pLong))

    mycursor = connection.cursor()
    sql = "UPDATE `psgc_data` SET latitude = %s, longitude = %s, processed = %s WHERE id = %s"
    
    val = (pLat, pLong, 2, code)
    mycursor.execute(sql, val)
    connection.commit()

def process_place(connection, code, barangay, city, province):
    if barangay is None:
        query = city + "," + province;
    else:
        barangay = re.sub('[^a-zA-Z0-9\n\.]', ' ', barangay)
        city = re.sub('[^a-zA-Z0-9\n\.]', ' ', city)
        query = barangay + "," + city + "," + province;
    
    data = do_geocode(query)
    
    if data is None:
        print(query + ': Not found')
        append_location(connection, code, '', '')
    else:
        lat = data.latitude
        long = data.longitude
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
        print("Connection successful... Fetching data from Mapbox")
        
        process_data(connection)
                
except Error as e:
    print(e)