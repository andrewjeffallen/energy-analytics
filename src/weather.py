import requests
import json
from geopy.geocoders import Nominatim

class Weather:

  def __init__(self, locale):
    self.geolocator = Nominatim(user_agent="python")
    self.location = self.geolocator.geocode(str(locale))
    self.lat = self.location.latitude
    self.lon = self.location.longitude
    self.url = "https://api.weather.gov/points/" + str(self.lat) + "," + str(self.lon)

  def getWeather(self):
    forecast_url = json.loads(requests.get(self.url).content)["properties"]["forecastHourly"]
    res = json.loads(requests.get(forecast_url).content)
    print(self.location)
    print("Current temperature is: " + str(res["properties"]["periods"][0]["temperature"]) + "°F")
    print("Windspeed: " + str(res["properties"]["periods"][0]["windSpeed"]))
    print("Current conditions: " + str(res["properties"]["periods"][0]["shortForecast"]))

if __name__ == "__main__":
  locale = input("\r\nZipcode, City/State: " )
  weather = Weather(locale)
  weather.getWeather()

