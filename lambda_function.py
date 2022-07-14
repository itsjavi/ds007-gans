import json
import requests
import apis

def df_to_json_serializable(df):
    return json.loads(json.dumps(df.to_dict('records'), default=str))


def lambda_handler(event, context):
    # apis.import_weather_data()
    weather_df = apis.get_weathers_df().tail(5)
   
    return {
        'weather': df_to_json_serializable(weather_df),
        'flights': '' # TODO
    }
