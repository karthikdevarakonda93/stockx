from pyowm.owm import OWM
from datetime import datetime, timedelta, timezone
import s3fs
import pandas as pd

# Connection Setup
#################################################
fs = s3fs.S3FileSystem(key='AKIA4VDLTN2YQ5GAEU64', secret='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')
BUCKET_NAME = 'stockx-bucket'
fs.ls(BUCKET_NAME)

owm = OWM("cbc2ea65cc405007f0cc8fb2819bb1ce")
mgr = owm.weather_manager()

# Function to request weather data from a Open Weather Data API
###################################################################################################################
def request_weather(city_name, day):
    lat, long = city_loc(city_name)
    timestamp = datetime.now()
    three_days_ago_epoch = int((datetime.now() - timedelta(days=day)).replace(tzinfo=timezone.utc).timestamp())
    one_call_three_days_ago = mgr.one_call_history(lat=lat, lon=long, dt=three_days_ago_epoch)
    list_of_forecasted_weathers = one_call_three_days_ago.forecast_hourly
    return city_name, datetime.fromtimestamp(list_of_forecasted_weathers[14].reference_time()), list_of_forecasted_weathers[14].detailed_status

# Simple lookup function
###################################################################################################################
def city_loc(city):
    if city == "Tallahassee":
        lat = 30.4383
        long = 84.2807
        return lat, long
    elif city == "Detroit":
        lat = 42.3314
        long = 83.0458
        return lat, long

# Create a Dataframe
df = pd.DataFrame()

list_of_cites = ["Tallahassee", "Detroit"]

# iterate over the list of cities and store them in a dataframe
for i in list_of_cites:
    for j in range(5):
        name, date, weather = request_weather(i, j)
        date = date.strftime("%m-%d-%Y")
        new_row = pd.Series(data={'city': i, 'date': date, 'weather': weather}, name='Weather_Data')
        # append row to the dataframe
        df = df.append(new_row, ignore_index=False)

df.insert(0, 'ID', range(0, len(df)))
print(df)

# Send it to the S3 Bucket as csv file
##########################################################################################################
with fs.open(f"{BUCKET_NAME}/data.csv", 'w') as f:
    df.to_csv(f)


"""""
Commands to load this Datafile from S3 to Redshift:

1) Create an IAM Role in AWS RedShift.
2) Navigate to your AWS Redshift service home page and click on the Cluster tab on the left pane. 
Assuming you have a working cluster. Add the IAM Role.

Use COPY Command:
copy weather (id, city, date, weather)
from ‘s3://stockx-bucket/data'
iam_role ‘arn:aws:iam::ROLE_ID:role/S3_Access_Role_For_Redshift’
Csv
IGNOREHEADER 1 

"""""