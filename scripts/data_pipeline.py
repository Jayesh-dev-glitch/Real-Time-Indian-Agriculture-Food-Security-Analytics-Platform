import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

class AgriSenseDataManager:
    def __init__(self):
        self.raw_path = "raw_data"
        self.processed_path = "processed_data"
        
        # Create directories
        os.makedirs(self.raw_path, exist_ok=True)
        os.makedirs(self.processed_path, exist_ok=True)
    
    def generate_sample_data(self):
        """
        Sample commodity data generate karo for testing
        (Real API data ke liye data_collector.py use karo)
        """
        print("📊 Generating sample commodity data...")
        
        # Sample data
        commodities = ['Wheat', 'Rice', 'Tomato', 'Onion', 'Potato', 'Cotton', 'Sugarcane']
        states = ['Punjab', 'Haryana', 'Maharashtra', 'Karnataka', 'Tamil Nadu', 'Uttar Pradesh']
        markets = ['APMC Market', 'Mandi', 'Wholesale Market', 'Agricultural Market']
        
        # Generate 1000 sample records
        data = []
        base_date = datetime.now() - timedelta(days=180)
        
        for i in range(1000):
            commodity = random.choice(commodities)
            state = random.choice(states)
            market = random.choice(markets)
            date = base_date + timedelta(days=random.randint(0, 180))
            
            # Base prices for different commodities
            base_prices = {
                'Wheat': 2000, 'Rice': 2500, 'Tomato': 30, 
                'Onion': 25, 'Potato': 20, 'Cotton': 5000, 'Sugarcane': 300
            }
            
            base_price = base_prices.get(commodity, 1000)
            price_variation = random.uniform(-0.3, 0.3)
            modal_price = base_price * (1 + price_variation)
            
            data.append({
                'commodity': commodity,
                'state': state,
                'market': market,
                'arrival_date': date.strftime('%Y-%m-%d'),
                'modal_price': round(modal_price, 2),
                'price': round(modal_price, 2),
                'min_price': round(modal_price * 0.9, 2),
                'max_price': round(modal_price * 1.1, 2)
            })
        
        df = pd.DataFrame(data)
        
        # Save to raw_data folder
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.raw_path}/commodity_prices_{timestamp}.csv"
        df.to_csv(filename, index=False)
        
        print(f"✅ Sample data generated: {filename}")
        print(f"   Total records: {len(df)}")
        print(f"   Date range: {df['arrival_date'].min()} to {df['arrival_date'].max()}")
        return filename
    
    def clean_commodity_data(self, filepath):
        """
        Raw commodity data ko clean aur standardize karo
        """
        try:
            print(f"\n🧹 Cleaning data from: {filepath}")
            df = pd.read_csv(filepath)
            
            # Data cleaning steps
            # 1. Missing values handle karo
            df = df.dropna(subset=['commodity', 'market', 'price'])
            print(f"✅ After removing missing values: {len(df)} rows")
            
            # 2. Date format standardize karo
            df['date'] = pd.to_datetime(df['arrival_date'], errors='coerce')
            
            # 3. Price ko numeric convert karo
            df['modal_price'] = pd.to_numeric(df['modal_price'], errors='coerce')
            df = df.dropna(subset=['modal_price'])
            
            # 4. Outliers remove karo (IQR method)
            Q1 = df['modal_price'].quantile(0.25)
            Q3 = df['modal_price'].quantile(0.75)
            IQR = Q3 - Q1
            
            if IQR > 0:
                df = df[~((df['modal_price'] < (Q1 - 1.5 * IQR)) | 
                          (df['modal_price'] > (Q3 + 1.5 * IQR)))]
                print(f"✅ After removing outliers: {len(df)} rows")
            
            # 5. State-wise aggregation
            df['state'] = df['state'].str.strip().str.title()
            df['commodity'] = df['commodity'].str.strip().str.title()
            
            # Save processed data
            output_file = f"{self.processed_path}/clean_commodity_prices.csv"
            df.to_csv(output_file, index=False)
            print(f"✅ Cleaned data saved: {output_file}")
            return df
            
        except Exception as e:
            print(f"❌ Error in cleaning: {e}")
            return None
    
    def create_features(self, df):
        """
        ML ke liye features engineer karo
        """
        try:
            print(f"\n🔧 Creating features...")
            df = df.copy()
            df = df.sort_values(['commodity', 'date'])
            
            # 1. Rolling averages (7-day, 30-day)
            df['price_7day_avg'] = df.groupby('commodity')['modal_price'].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )
            df['price_30day_avg'] = df.groupby('commodity')['modal_price'].transform(
                lambda x: x.rolling(window=30, min_periods=1).mean()
            )
            
            # 2. Price change percentage
            df['price_change_pct'] = df.groupby('commodity')['modal_price'].pct_change() * 100
            df['price_change_pct'] = df['price_change_pct'].fillna(0)
            
            # 3. Seasonality features
            df['month'] = df['date'].dt.month
            df['quarter'] = df['date'].dt.quarter
            df['day_of_year'] = df['date'].dt.dayofyear
            df['week_of_year'] = df['date'].dt.isocalendar().week
            
            # 4. Volatility index
            df['volatility'] = df.groupby('commodity')['modal_price'].transform(
                lambda x: x.rolling(window=30, min_periods=1).std()
            )
            df['volatility'] = df['volatility'].fillna(0)
            
            # 5. Price comparison features
            df['price_vs_7day_avg'] = ((df['modal_price'] - df['price_7day_avg']) / df['price_7day_avg'] * 100).fillna(0)
            df['price_vs_30day_avg'] = ((df['modal_price'] - df['price_30day_avg']) / df['price_30day_avg'] * 100).fillna(0)
            
            # 6. Trend indicator
            df['trend'] = np.where(df['price_change_pct'] > 2, 'Rising',
                                   np.where(df['price_change_pct'] < -2, 'Falling', 'Stable'))
            
            output_file = f"{self.processed_path}/featured_data.csv"
            df.to_csv(output_file, index=False)
            print(f"✅ Featured data saved: {output_file}")
            
            # Summary
            print(f"\n📊 Feature Summary:")
            print(f"   Total rows: {len(df)}")
            print(f"   Total features: {len(df.columns)}")
            print(f"   Commodities: {df['commodity'].nunique()}")
            print(f"   Date range: {df['date'].min().date()} to {df['date'].max().date()}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error in feature creation: {e}")
            return None
    
    def generate_insights(self, df):
        """
        Data se insights nikalo
        """
        print(f"\n📈 Generating Insights...\n")
        
        # 1. Top 5 most expensive commodities
        print("💰 Top 5 Most Expensive Commodities (Average Price):")
        top_expensive = df.groupby('commodity')['modal_price'].mean().sort_values(ascending=False).head(5)
        for commodity, price in top_expensive.items():
            print(f"   {commodity}: ₹{price:.2f}")
        
        # 2. Most volatile commodities
        print("\n📉 Most Volatile Commodities:")
        volatility = df.groupby('commodity')['volatility'].mean().sort_values(ascending=False).head(5)
        for commodity, vol in volatility.items():
            print(f"   {commodity}: {vol:.2f}")
        
        # 3. State-wise average prices
        print("\n🗺️  State-wise Average Prices:")
        state_prices = df.groupby('state')['modal_price'].mean().sort_values(ascending=False).head(5)
        for state, price in state_prices.items():
            print(f"   {state}: ₹{price:.2f}")
        
        # 4. Recent trends
        recent_data = df[df['date'] >= df['date'].max() - timedelta(days=7)]
        print("\n📊 Recent 7-Day Trends:")
        trends = recent_data.groupby('commodity')['price_change_pct'].mean().sort_values(ascending=False)
        for commodity, change in trends.head(3).items():
            print(f"   {commodity}: {'+' if change > 0 else ''}{change:.2f}%")

# Main execution
if __name__ == "__main__":
    print("🚀 AgriSense Data Management System\n")
    print("="*50)
    
    manager = AgriSenseDataManager()
    
    # Step 1: Generate sample data
    print("\nStep 1: Data Collection")
    print("-"*50)
    filepath = manager.generate_sample_data()
    
    # Step 2: Clean data
    print("\nStep 2: Data Cleaning")
    print("-"*50)
    clean_df = manager.clean_commodity_data(filepath)
    
    if clean_df is not None:
        # Step 3: Create features
        print("\nStep 3: Feature Engineering")
        print("-"*50)
        featured_df = manager.create_features(clean_df)
        
        if featured_df is not None:
            # Step 4: Generate insights
            print("\nStep 4: Insights Generation")
            print("-"*50)
            manager.generate_insights(featured_df)
            
            print("\n" + "="*50)
            print("✨ Pipeline completed successfully!")
            print("="*50)
            print(f"\n📁 Output Files:")
            print(f"   1. {manager.processed_path}/clean_commodity_prices.csv")
            print(f"   2. {manager.processed_path}/featured_data.csv")
        else:
            print("\n⚠️  Feature creation failed")
    else:
        print("\n⚠️  Data cleaning failed")