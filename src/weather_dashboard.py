import os
import json
import boto3
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class WeatherApp:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        self.client = boto3.client('s3')

    def create_bucket(self):
        """ Create S3 bucket if it does not exist """
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} already exists")
            print("Skipping creation")
            print("Proceeding to get weather data...")
            print()
            return True
        except self.client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': 'us-west-2'
                        }
                    )
                    print(f"Bucket {self.bucket_name} created successfully")
                except self.client.exceptions.ClientError as e:
                    print(f"Error creating bucket: {e}")
            else:
                return f"Error checking bucket: {e}"
    
    def get_weather(self, city):
        """ Get weather data from OpenWeather API """
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'imperial'
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting weather data: {e}")
            return None
    
    def save_to_s3(self, data, city):
        """ Save weather data to S3 bucket """
        if not data:
            return False
        
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        file_name = f'weather-data/{city}-{timestamp}.json'

        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print(f"Weather Data for {city} saved to: {file_name}")
            return True
        except Exception as e:
            print(f"Error saving data to S3 bucket: {e}")
            return False

def main():
    try:
        app = WeatherApp()
        app.create_bucket()
        city = input("Enter city names separated by commas: ")
        cities = city.split(',')
        for city in cities:
            print(f"Getting weather data for {city}...")
            weather_data = app.get_weather(city)
            if weather_data:
                app.save_to_s3(weather_data, city)
                description = weather_data["weather"][0]["description"]
                temperature = weather_data["main"]["temp"]
                feels_like = weather_data["main"]["feels_like"]
                humidity = weather_data["main"]["humidity"]
                print (f"Temperature: {temperature}°F")
                print (f"Feels like: {feels_like}°F")
                print (f"Humidity: {humidity}%")
                print (f"Conditions: {description.title()}")
                print(f"Weather Data processed successfully for {city}")
                print()
            else:
                print(f"Failed to process {city} No data saved")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    main()            
