import pymysql
import pandas as pd
import requests
from datetime import datetime, date, timedelta
from pytz import timezone

# local imports:
import constants

def get_cities_df():
    table_df = pd.read_sql_table("cities", con=constants.DB_CONN_URI)
    return table_df[:1] # just one

def get_airports_df():
    table_df = pd.read_sql_table("airports", con=constants.DB_CONN_URI)
    return table_df[:1] # just one

def get_weathers_df():
    table_df = pd.read_sql_table("weathers", con=constants.DB_CONN_URI)
    return table_df

def get_flights_df():
    table_df = pd.read_sql_table("flights", con=constants.DB_CONN_URI)
    return table_df

def fetch_weather_data(cities):
    weather_list = []
    
    for index, row in cities.iterrows():
        city = row['city']
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={constants.OPENWEATHER_API_KEY}&units=metric"
        res = requests.get(url)
        
        if res.status_code != 200:
            print("City = ")
            print(row)
            print(res.json())
            raise Exception(f"Unexpected open weather API status code: {res.status_code}")
            
        weather_df = pd.json_normalize(res.json()["list"])
        weather_df["city"] = city
        weather_df["city_id"] = row['city_id']
        weather_list.append(weather_df)

    weather_combined = pd.concat(weather_list, ignore_index = True)
    
    # set dataframe to keep only the columns of interest
    weather_combined = weather_combined[["pop", "dt_txt", "main.temp", "main.feels_like", "main.humidity", "clouds.all", 
                                 "wind.speed", "wind.gust", "city", "city_id"]]
                             
    weather_combined.rename(columns = {"pop": "precip_prob", 
                               "dt_txt": "forecast_time", 
                               "main.temp": "temperature",
                               "main.feels_like": "feels_like",
                               "main.humidity": "humidity", 
                               "clouds.all": "cloudiness", 
                               "wind.speed": "wind_speed", 
                               "wind.gust": "wind_gust",}, 
                    inplace = True)
    
    weather_combined["forecast_time"] = pd.to_datetime(weather_combined["forecast_time"])
    
    return weather_combined
    

def fetch_icao_airport_code(latitude, longitude):
    airport_list = []
    # check the length of the latitude and longitude lists to make sure they are equal
    assert len(latitude) == len(longitude)
    headers = {
        "X-RapidAPI-Key": constants.RAPIDAPI_API_KEY,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    
    for i in range(len(latitude)):
        # set the API call to get airport data within 50km of the lat and lon being input and show 10 results
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{latitude[i]}/{longitude[i]}/km/50/10"
        querystring = {"withFlightInfoOnly":"true"}
        headers = {
            "X-RapidAPI-Key": API_key, 
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers = headers, params = querystring)
        
        if response.status_code != 200:
            print(response.json())
            raise Exception(f"Unexpected aerodatabox API status code: {response.status_code}")
        
        airport_df = pd.json_normalize(response.json()["items"])
        airport_list.append(airport_df)
        
    airports_df = pd.concat(airport_list, ignore_index = True)
    return airports_df


def fetch_flight_arrivals_data(icao_codes):
    # use the datetime function in python to get today's and tomorrow's date to be used as inputs of the API call
    today = datetime.now().astimezone(timezone("Europe/Berlin")).date()
    tomorrow = (today + timedelta(days = 1))
    arrival_list = []
    headers = {
        "X-RapidAPI-Key": constants.RAPIDAPI_API_KEY,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }

    for code in icao_codes:
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{code}/{tomorrow}T10:00/{tomorrow}T22:00"
        querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false",
                       "withCodeshared":"true","withCargo":"false","withPrivate":"false",
                       "withLocation":"false"}
        headers = {
            "X-RapidAPI-Key": API_key,
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers = headers, params = querystring)
        
        if response.status_code != 200:
            print(response.json())
            raise Exception(f"Unexpected aerodatabox API status code: {response.status_code}")
        
        arrival_df = pd.json_normalize(response.json()["arrivals"])
        arrival_df["arrival_icao"] = code
        arrival_list.append(arrival_df)
    
    arrivals_df = pd.concat(arrival_list, ignore_index = True)  
    return arrivals_df
    
    
def import_weather_data():
    city_df = get_cities_df()
    weather_df = fetch_weather_data(city_df)
    
    #city_df.drop(columns = ["Unnamed: 0"], inplace = True)
    #weather_df.drop(columns = ["Unnamed: 0"], inplace = True)
    
    weather_df.to_sql("weathers", if_exists = "append", con = constants.DB_CONN_URI, index = False)
