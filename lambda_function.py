import json
import requests
import apis

def lambda_handler(event, context):
    apis.import_weather_data()
    weather_df = apis.get_weathers_df().tail(5)
   
    return {
        'weather': json.loads(weather_df.to_json()),
        'flights': '' # TODO
    }
