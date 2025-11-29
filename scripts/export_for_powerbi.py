import pandas as pd
import os

def prepare_powerbi_data():
    """
    Power BI ke liye optimized data prepare karo
    """
    try:
        # Main featured data load karo
        df = pd.read_csv("processed_data/featured_data.csv")
        
        # Date column ko proper format mein convert karo
        df['date'] = pd.to_datetime(df['date'])
        
        # Power BI ke liye separate tables banao
        
        # 1. Fact Table - Price Data
        fact_prices = df[['date', 'commodity', 'state', 'market', 
                          'modal_price', 'price_7day_avg', 'price_30day_avg',
                          'price_change_pct', 'volatility']].copy()
        fact_prices.to_csv("processed_data/powerbi_fact_prices.csv", index=False)
        print("✅ Fact table created: powerbi_fact_prices.csv")
        
        # 2. Dimension Table - Commodities
        dim_commodities = df[['commodity']].drop_duplicates().reset_index(drop=True)
        dim_commodities['commodity_id'] = dim_commodities.index + 1
        dim_commodities.to_csv("processed_data/powerbi_dim_commodities.csv", index=False)
        print("✅ Dimension table created: powerbi_dim_commodities.csv")
        
        # 3. Dimension Table - States
        dim_states = df[['state']].drop_duplicates().reset_index(drop=True)
        dim_states['state_id'] = dim_states.index + 1
        dim_states.to_csv("processed_data/powerbi_dim_states.csv", index=False)
        print("✅ Dimension table created: powerbi_dim_states.csv")
        
        # 4. Dimension Table - Date (for time intelligence)
        dim_date = pd.DataFrame({
            'date': pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
        })
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
        dim_date['quarter'] = dim_date['date'].dt.quarter
        dim_date['day'] = dim_date['date'].dt.day
        dim_date['day_name'] = dim_date['date'].dt.strftime('%A')
        dim_date.to_csv("processed_data/powerbi_dim_date.csv", index=False)
        print("✅ Dimension table created: powerbi_dim_date.csv")
        
        # 5. Summary Statistics Table
        summary_stats = df.groupby('commodity').agg({
            'modal_price': ['mean', 'min', 'max', 'std'],
            'volatility': 'mean',
            'state': 'count'
        }).reset_index()
        summary_stats.columns = ['commodity', 'avg_price', 'min_price', 
                                 'max_price', 'price_std', 'avg_volatility', 'record_count']
        summary_stats.to_csv("processed_data/powerbi_summary_stats.csv", index=False)
        print("✅ Summary stats created: powerbi_summary_stats.csv")
        
        print("\n🎉 All Power BI tables created successfully!")
        print(f"📊 Total records: {len(fact_prices)}")
        print(f"🌾 Total commodities: {len(dim_commodities)}")
        print(f"🗺️ Total states: {len(dim_states)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    prepare_powerbi_data()