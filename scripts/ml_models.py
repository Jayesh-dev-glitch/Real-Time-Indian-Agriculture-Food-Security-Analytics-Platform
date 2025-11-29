import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor  # type: ignore
import pickle
import numpy as np

class PricePredictor:
    def __init__(self):
        self.model = None
        self.processed_path = "processed_data"
        self.model_path = "models"
    
    def train_price_prediction_model(self):
        """
        Commodity price prediction model train karo
        """
        # Featured data load karo
        df = pd.read_csv(f"{self.processed_path}/featured_data.csv")
        
        # Features select karo
        feature_cols = ['price_7day_avg', 'price_30day_avg', 
                        'month', 'quarter', 'volatility']
        target_col = 'modal_price'
        
        # Prepare data
        df = df.dropna(subset=feature_cols + [target_col])
        X = df[feature_cols]
        y = df[target_col]
        
        # Train-test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # XGBoost model train karo
        model = XGBRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        # Evaluation
        train_score = model.score(X_train, y_train)
        test_score = model.score(X_test, y_test)
        
        print(f"✅ Model trained successfully!")
        print(f"   Train R² Score: {train_score:.3f}")
        print(f"   Test R² Score: {test_score:.3f}")
        
        # Save model
        with open(f"{self.model_path}/price_predictor.pkl", 'wb') as f:
            pickle.dump(model, f)
        
        self.model = model
        return model
    
    def predict_next_week_prices(self, commodity_name):
        """
        Next week ka price predict karo
        """
        # Model load karo
        with open(f"{self.model_path}/price_predictor.pkl", 'rb') as f:
            model = pickle.load(f)
        
        # Latest data fetch karo
        df = pd.read_csv(f"{self.processed_path}/featured_data.csv")
        df = df[df['commodity'] == commodity_name].tail(1)
        
        # Features prepare karo
        features = df[['price_7day_avg', 'price_30day_avg', 
                       'month', 'quarter', 'volatility']]
        
        # Prediction
        predicted_price = model.predict(features)[0]
        
        return predicted_price

# Run karo
if __name__ == "__main__":
    predictor = PricePredictor()
    predictor.train_price_prediction_model()