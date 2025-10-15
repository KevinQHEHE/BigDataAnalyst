import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://air-quality-api.open-meteo.com/v1/air-quality"
params = {
	"latitude": 10.82,
	"longitude": 106.63,
	"hourly": ["aerosol_optical_depth", "pm2_5", "pm10", "dust", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide", "uv_index", "uv_index_clear_sky", "carbon_dioxide", "us_aqi", "us_aqi_pm2_5", "us_aqi_nitrogen_dioxide", "us_aqi_pm10", "us_aqi_carbon_monoxide", "us_aqi_ozone", "us_aqi_sulphur_dioxide"],
	"utm_source": "chatgpt.com",
	"bounding_box": "-90,-180,90,180",
	"start_date": "2015-01-01",
	"end_date": "2025-08-01",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_aerosol_optical_depth = hourly.Variables(0).ValuesAsNumpy()
hourly_pm2_5 = hourly.Variables(1).ValuesAsNumpy()
hourly_pm10 = hourly.Variables(2).ValuesAsNumpy()
hourly_dust = hourly.Variables(3).ValuesAsNumpy()
hourly_nitrogen_dioxide = hourly.Variables(4).ValuesAsNumpy()
hourly_ozone = hourly.Variables(5).ValuesAsNumpy()
hourly_sulphur_dioxide = hourly.Variables(6).ValuesAsNumpy()
hourly_carbon_monoxide = hourly.Variables(7).ValuesAsNumpy()
hourly_uv_index = hourly.Variables(8).ValuesAsNumpy()
hourly_uv_index_clear_sky = hourly.Variables(9).ValuesAsNumpy()
hourly_carbon_dioxide = hourly.Variables(10).ValuesAsNumpy()
hourly_us_aqi = hourly.Variables(11).ValuesAsNumpy()
hourly_us_aqi_pm2_5 = hourly.Variables(12).ValuesAsNumpy()
hourly_us_aqi_nitrogen_dioxide = hourly.Variables(13).ValuesAsNumpy()
hourly_us_aqi_pm10 = hourly.Variables(14).ValuesAsNumpy()
hourly_us_aqi_carbon_monoxide = hourly.Variables(15).ValuesAsNumpy()
hourly_us_aqi_ozone = hourly.Variables(16).ValuesAsNumpy()
hourly_us_aqi_sulphur_dioxide = hourly.Variables(17).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["aerosol_optical_depth"] = hourly_aerosol_optical_depth
hourly_data["pm2_5"] = hourly_pm2_5
hourly_data["pm10"] = hourly_pm10
hourly_data["dust"] = hourly_dust
hourly_data["nitrogen_dioxide"] = hourly_nitrogen_dioxide
hourly_data["ozone"] = hourly_ozone
hourly_data["sulphur_dioxide"] = hourly_sulphur_dioxide
hourly_data["carbon_monoxide"] = hourly_carbon_monoxide
hourly_data["uv_index"] = hourly_uv_index
hourly_data["uv_index_clear_sky"] = hourly_uv_index_clear_sky
hourly_data["carbon_dioxide"] = hourly_carbon_dioxide
hourly_data["us_aqi"] = hourly_us_aqi
hourly_data["us_aqi_pm2_5"] = hourly_us_aqi_pm2_5
hourly_data["us_aqi_nitrogen_dioxide"] = hourly_us_aqi_nitrogen_dioxide
hourly_data["us_aqi_pm10"] = hourly_us_aqi_pm10
hourly_data["us_aqi_carbon_monoxide"] = hourly_us_aqi_carbon_monoxide
hourly_data["us_aqi_ozone"] = hourly_us_aqi_ozone
hourly_data["us_aqi_sulphur_dioxide"] = hourly_us_aqi_sulphur_dioxide

hourly_dataframe = pd.DataFrame(data = hourly_data)
print("\nHourly data\n", hourly_dataframe)