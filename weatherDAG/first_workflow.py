#import required libraries
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json
import pandas as pd


#function that pulls data from weather API
def pre_processor_func():
    api_key = ""      #API key associated with my OpenWeatherMap account
    #replace API key with your own API key given by OpenWeatherMap
    lat = "31.520370"  
    lon = "74.358749"

    #API call
    url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&units=metric" % (lat, lon, api_key)
    response = requests.get(url)
    data = json.loads(response.text)
    json_object = json.dumps(data, indent=4)

    #filter data and keep only current readings
    current = data['current']
    json_object = json.dumps(current, indent = 4)

    #primary key in our database 
    key = current['dt']
    #create a dictionary with timestamp as the key and all the records as values
    a_dict = {current['dt']: current}
    del a_dict[current['dt']]['dt']

    #dump the dictionary into a json object
    json_object = json.dumps(a_dict, indent = 4)
    data.update(a_dict)

    #send the records in JSON file to individual variables
    record_id = key
    sunrise = a_dict[key]["sunrise"]
    sunset = a_dict[key]["sunset"]
    temperature = a_dict[key]["temp"]
    feels_like = a_dict[key]["feels_like"]
    pressure = a_dict[key]["pressure"]
    humidity = a_dict[key]["humidity"]
    dew_point = a_dict[key]["dew_point"]
    uvi = a_dict[key]["uvi"]
    clouds = a_dict[key]["clouds"]
    visibility = a_dict[key]["visibility"]
    wind_speed = a_dict[key]["wind_speed"]
    wind_deg = a_dict[key]["wind_deg"]
    print(key)


    #create dataframe of the received data
    df = pd.DataFrame(columns=["record_id", "sunrise", "sunset", "temperature", "feels_like", "pressure", 
                    "humidity", "dew_point", "uvi", "clouds", "visibility", 
                    "wind_speed", "wind_deg"])
    df.loc[len(df.index)] = [key, sunrise, sunset, temperature, 
                                        feels_like, pressure, humidity, dew_point, 
                                        uvi, clouds, visibility, wind_speed, wind_deg]
    #append the new record to csv file
    df.to_csv("~/op_files/weather.csv", mode='a', index = False, header=False)


#set the default arguments of the DAG
default_args = {"owner":"airflow","start_date":datetime(2022,3,7)}

#create DAG object
#CRON expression has been set so that the DAG runs every minute, can be changed
#catchup is False so the DAG doesn't have to catch up for any lost time
with DAG(dag_id="workflow",default_args=default_args,schedule_interval='* * * * *', catchup = False) as dag:
    
    #operator checks if the file exists
    #retries 2 times at 15 second intervals if operator doesn't work
    check_file = BashOperator(
        task_id="check_file",
        bash_command="shasum ~/ip_files/weather.csv",
        retries = 2,
        retry_delay=timedelta(seconds=15))
    
    #Python Operator that calls the function where the API pull occurred
    pre_process_func = PythonOperator(
        task_id = "pre_process",
        python_callable = pre_processor_func
        )
    #printing a log to check if process has been completed properly
    print("Done")

    #final step: setting up DAG dependencies
    check_file >> pre_process_func