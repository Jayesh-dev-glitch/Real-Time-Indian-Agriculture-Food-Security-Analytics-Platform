import dash # type: ignore
from dash import dcc, html, Input, Output # type: ignore
import plotly.graph_objects as go # type: ignore
import pandas as pd
import numpy as np
from datetime import timedelta

# Initialize app
app = dash.Dash(__name__)
app.title = "AgriSense Dashboard"

# Load data with error handling
try:
    df = pd.read_csv("processed_data/featured_data.csv")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date', 'modal_price'])
    df = df.sort_values(['commodity', 'date'])
    print(f"Data loaded: {len(df)} rows, {df['commodity'].nunique()} commodities")
except Exception as e:
    print(f"Error loading data: {e}")
    df = pd.DataFrame()

# Get unique commodities
commodities = sorted(df['commodity'].unique().tolist()) if not df.empty else []

# Dashboard layout
app.layout = html.Div([
    html.Div([
        html.H1("🌾 AgriSense - Indian Agriculture Analytics", 
                style={
                    'textAlign': 'center', 
                    'color': '#2E7D32',
                    'padding': '15px',
                    'backgroundColor': '#F1F8E9',
                    'margin': '0 0 20px 0',
                    'borderRadius': '8px',
                    'fontSize': '28px'
                })
    ]),
    
    html.Div([
        html.H3("📊 Commodity Price Trends", 
                style={'color': '#1B5E20', 'marginBottom': '10px', 'fontSize': '20px'}),
        dcc.Dropdown(
            id='commodity-dropdown',
            options=[{'label': c, 'value': c} for c in commodities],
            value=commodities[0] if commodities else None,
            clearable=False,
            style={'marginBottom': '15px'}
        ),
        dcc.Graph(id='price-trend-graph', config={'displayModeBar': False})
    ], style={
        'marginBottom': '25px', 
        'padding': '15px', 
        'backgroundColor': '#FFFFFF', 
        'borderRadius': '8px', 
        'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
    }),
    
    html.Div([
        html.H3("🗺️ State-wise Price Comparison", 
                style={'color': '#1B5E20', 'marginBottom': '10px', 'fontSize': '20px'}),
        dcc.Dropdown(
            id='state-commodity-dropdown',
            options=[{'label': c, 'value': c} for c in commodities],
            value=commodities[0] if commodities else None,
            clearable=False,
            style={'marginBottom': '15px'}
        ),
        dcc.Graph(id='state-comparison-graph', config={'displayModeBar': False})
    ], style={
        'marginBottom': '25px', 
        'padding': '15px', 
        'backgroundColor': '#FFFFFF', 
        'borderRadius': '8px', 
        'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
    }),
    
    html.Div([
        html.H3("📈 Price Prediction (Next 7 Days)", 
                style={'color': '#1B5E20', 'marginBottom': '10px', 'fontSize': '20px'}),
        dcc.Dropdown(
            id='prediction-commodity-dropdown',
            options=[{'label': c, 'value': c} for c in commodities],
            value=commodities[0] if commodities else None,
            clearable=False,
            style={'marginBottom': '15px'}
        ),
        dcc.Graph(id='prediction-graph', config={'displayModeBar': False})
    ], style={
        'marginBottom': '25px', 
        'padding': '15px', 
        'backgroundColor': '#FFFFFF', 
        'borderRadius': '8px', 
        'boxShadow': '0 2px 4px rgba(0,0,0,0.08)'
    }),
    
], style={'padding': '15px', 'backgroundColor': '#E8F5E9', 'minHeight': '100vh'})

@app.callback(
    Output('price-trend-graph', 'figure'),
    Input('commodity-dropdown', 'value')
)
def update_price_trend(selected_commodity):
    if df.empty or not selected_commodity:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    try:
        filtered_df = df[df['commodity'] == selected_commodity].copy()
        
        if len(filtered_df) > 90:
            filtered_df = filtered_df.tail(90)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=filtered_df['date'], 
            y=filtered_df['modal_price'],
            mode='lines+markers',
            name='Modal Price',
            line=dict(color='#2E7D32', width=2),
            marker=dict(size=4),
            hovertemplate='<b>Date:</b> %{x|%d %b %Y}<br><b>Price:</b> ₹%{y:.2f}<extra></extra>'
        ))
        
        if 'price_30day_avg' in filtered_df.columns:
            fig.add_trace(go.Scatter(
                x=filtered_df['date'],
                y=filtered_df['price_30day_avg'],
                mode='lines',
                name='30-Day Avg',
                line=dict(color='#FF6F00', width=2, dash='dash'),
                hovertemplate='<b>Date:</b> %{x|%d %b %Y}<br><b>Avg:</b> ₹%{y:.2f}<extra></extra>'
            ))
        
        fig.update_layout(
            title=dict(text=f"<b>{selected_commodity}</b> - Price Trend", font=dict(size=18)),
            xaxis_title="Date",
            yaxis_title="Price (₹/Quintal)",
            hovermode='x unified',
            template='plotly_white',
            height=400,
            margin=dict(l=50, r=30, t=50, b=50),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        return fig
    except Exception as e:
        print(f"Error in price trend: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"Error: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('state-comparison-graph', 'figure'),
    Input('state-commodity-dropdown', 'value')
)
def update_state_comparison(selected_commodity):
    if df.empty or not selected_commodity:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    try:
        filtered_df = df[df['commodity'] == selected_commodity]
        
        if 'state' not in filtered_df.columns:
            fig = go.Figure()
            fig.add_annotation(text="State data not available", showarrow=False, font=dict(size=16))
            return fig
        
        state_avg = filtered_df.groupby('state')['modal_price'].mean().sort_values(ascending=False).head(12)
        
        fig = go.Figure(data=[
            go.Bar(
                x=state_avg.index,
                y=state_avg.values,
                marker=dict(
                    color=state_avg.values,
                    colorscale='Greens',
                    showscale=False
                ),
                text=[f'₹{v:.2f}' for v in state_avg.values],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Avg Price: ₹%{y:.2f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title=dict(text=f"<b>{selected_commodity}</b> - Average Price by State", font=dict(size=18)),
            xaxis_title="State",
            yaxis_title="Average Price (₹/Quintal)",
            xaxis_tickangle=-45,
            template='plotly_white',
            height=400,
            margin=dict(l=50, r=30, t=50, b=100)
        )
        return fig
    except Exception as e:
        print(f"Error in state comparison: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"Error: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

@app.callback(
    Output('prediction-graph', 'figure'),
    Input('prediction-commodity-dropdown', 'value')
)
def update_prediction(selected_commodity):
    if df.empty or not selected_commodity:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False, font=dict(size=16))
        return fig
    
    try:
        filtered_df = df[df['commodity'] == selected_commodity].copy()
        
        if len(filtered_df) < 2:
            fig = go.Figure()
            fig.add_annotation(text="Insufficient data for prediction", showarrow=False, font=dict(size=16))
            return fig
        
        last_30 = filtered_df.tail(30)
        
        if len(last_30) == 0:
            fig = go.Figure()
            fig.add_annotation(text="No recent data available", showarrow=False, font=dict(size=16))
            return fig
        
        last_date = last_30['date'].max()
        last_prices = last_30['modal_price'].dropna().values
        
        if len(last_prices) < 2:
            fig = go.Figure()
            fig.add_annotation(text="Insufficient price data", showarrow=False, font=dict(size=16))
            return fig
        
        trend = np.polyfit(range(len(last_prices)), last_prices, 1)
        future_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
        predictions = [max(0, trend[0] * (len(last_prices) + i) + trend[1]) for i in range(1, 8)]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=last_30['date'],
            y=last_30['modal_price'],
            mode='lines+markers',
            name='Historical',
            line=dict(color='#2E7D32', width=2),
            marker=dict(size=4),
            hovertemplate='<b>%{x|%d %b}</b><br>₹%{y:.2f}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=future_dates,
            y=predictions,
            mode='lines+markers',
            name='Forecast',
            line=dict(color='#FF5722', width=2, dash='dash'),
            marker=dict(size=6, symbol='diamond'),
            hovertemplate='<b>%{x|%d %b}</b><br>₹%{y:.2f}<extra></extra>'
        ))
        
        fig.update_layout(
            title=dict(text=f"<b>{selected_commodity}</b> - 7-Day Price Forecast", font=dict(size=18)),
            xaxis_title="Date",
            yaxis_title="Price (₹/Quintal)",
            hovermode='x unified',
            template='plotly_white',
            height=400,
            margin=dict(l=50, r=30, t=50, b=50),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        return fig
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        fig = go.Figure()
        fig.add_annotation(text=f"Prediction error: {str(e)}", showarrow=False, font=dict(size=14))
        return fig

if __name__ == '__main__':
    print("\nStarting AgriSense Dashboard...")
    print("Open browser at: http://localhost:8050\n")
    app.run_server(debug=True, port=8050, host='0.0.0.0')