import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

class DataCollector:
    def __init__(self):
        self.data_gov_key = os.getenv('DATA_GOV_API_KEY', '')
        self.weather_key = os.getenv('OPENWEATHER_API_KEY', '')
        
        # Set timeout for all requests (in seconds)
        self.timeout = 30
        
        # Check if API keys are set
        if not self.data_gov_key or self.data_gov_key == 'YOUR_API_KEY':
            print("⚠️  Warning: DATA_GOV_API_KEY not set in .env file")
        
        if not self.weather_key or self.weather_key == 'YOUR_OPENWEATHER_KEY':
            print("⚠️  Warning: OPENWEATHER_API_KEY not set in .env file")
    
    def fetch_crop_data(self):
        """Fetch crop production data from India Data Portal"""
        try:
            if not self.data_gov_key or self.data_gov_key == 'YOUR_API_KEY':
                print("❌ Cannot fetch crop data: Invalid API key")
                return None
            
            url = f"https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
            params = {
                'api-key': self.data_gov_key,
                'format': 'json',
                'limit': 10000
            }
            
            print("📡 Fetching crop data...")
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'records' in data:
                df = pd.DataFrame(data['records'])
                print(f"✅ Fetched {len(df)} crop records")
                return df
            else:
                print("⚠️  No records found in response")
                return None
                
        except requests.exceptions.Timeout:
            print("❌ Error: Request timed out. Check your internet connection.")
            return None
        except requests.exceptions.ConnectionError:
            print("❌ Error: Cannot connect to api.data.gov.in. Check your network/firewall.")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching crop data: {e}")
            return None
    
    def fetch_weather_data(self, cities=None):
        """Fetch weather data from OpenWeatherMap"""
        if cities is None:
            cities = ['Delhi', 'Mumbai', 'Bangalore', 'Chennai', 'Kolkata']
        
        try:
            if not self.weather_key or self.weather_key == 'YOUR_OPENWEATHER_KEY':
                print("❌ Cannot fetch weather data: Invalid API key")
                return None
            
            weather_data = []
            
            for city in cities:
                try:
                    print(f"🌤️  Fetching weather for {city}...")
                    url = f"https://api.openweathermap.org/data/2.5/weather"
                    params = {
                        'q': f'{city},IN',
                        'appid': self.weather_key,
                        'units': 'metric'
                    }
                    
                    response = requests.get(url, params=params, timeout=self.timeout)
                    response.raise_for_status()
                    
                    data = response.json()
                    weather_data.append({
                        'city': city,
                        'temperature': data['main']['temp'],
                        'humidity': data['main']['humidity'],
                        'pressure': data['main']['pressure'],
                        'weather': data['weather'][0]['description'],
                        'timestamp': datetime.now()
                    })
                    
                    # Small delay between requests to avoid rate limiting
                    time.sleep(0.5)
                    
                except requests.exceptions.RequestException as e:
                    print(f"⚠️  Error fetching weather for {city}: {e}")
                    continue
            
            if weather_data:
                df = pd.DataFrame(weather_data)
                print(f"✅ Fetched weather data for {len(df)} cities")
                return df
            else:
                print("❌ No weather data collected")
                return None
                
        except Exception as e:
            print(f"❌ Error in weather data collection: {e}")
            return None
    
    def save_data(self, df, filename):
        """Save data to CSV file"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs('data', exist_ok=True)
            
            filepath = os.path.join('data', filename)
            df.to_csv(filepath, index=False)
            print(f"💾 Data saved to {filepath}")
            return True
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return False

if __name__ == "__main__":
    print("🚀 Starting data collection...\n")
    
    collector = DataCollector()
    
    # Test connection with a simple request
    print("🔍 Testing internet connectivity...")
    try:
        requests.get("https://www.google.com", timeout=5)
        print("✅ Internet connection OK\n")
    except:
        print("❌ No internet connection detected\n")
    
    # Fetch crop data
    crop_df = collector.fetch_crop_data()
    if crop_df is not None:
        collector.save_data(crop_df, 'crop_data.csv')
    
    print()
    
    # Fetch weather data
    weather_df = collector.fetch_weather_data()
    if weather_df is not None:
        collector.save_data(weather_df, 'weather_data.csv')
    
    print("\n✨ Data collection complete!")