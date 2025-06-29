import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import streamlit as st
from typing import Dict, List, Optional
import numpy as np

class WeatherCharts:
    def __init__(self):
        self.color_palette = px.colors.qualitative.Set3
    
    def create_weather_impact_bar_chart(self, df: pd.DataFrame) -> go.Figure:
        fig = px.bar(
            df, 
            x='weather_condition', 
            y='incident_count',
            color='avg_severity',
            color_continuous_scale='Reds',
            title='Weather Conditions Impact on Incidents',
            labels={
                'incident_count': 'Number of Incidents',
                'weather_condition': 'Weather Condition',
                'avg_severity': 'Average Severity'
            }
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            showlegend=True
        )
        
        return fig
    
    def create_temporal_heatmap(self, df: pd.DataFrame) -> go.Figure:
        pivot_df = df.pivot_table(
            values='incident_count', 
            index='weather_condition', 
            columns='date', 
            fill_value=0
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale='YlOrRd',
            hoverongaps=False
        ))
        
        fig.update_layout(
            title='Incident Frequency Heatmap by Weather and Date',
            xaxis_title='Date',
            yaxis_title='Weather Condition',
            height=400
        )
        
        return fig
    
    def create_severity_distribution_pie(self, df: pd.DataFrame) -> go.Figure:
        severity_totals = df.groupby('severity_category')['count'].sum().reset_index()
        
        fig = px.pie(
            severity_totals,
            values='count',
            names='severity_category',
            title='Severity Distribution',
            color_discrete_sequence=px.colors.sequential.RdYlBu_r
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        
        return fig
    
    def create_correlation_scatter(self, df: pd.DataFrame, x_col: str, y_col: str) -> go.Figure:
        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            color='weather_condition',
            size='severity_score',
            hover_data=['severity_score'],
            title=f'{x_col.title()} vs {y_col.title()}',
            trendline='ols'
        )
        
        fig.update_layout(height=500)
        
        return fig
    
    def create_weather_metrics_radar(self, df: pd.DataFrame) -> go.Figure:
        weather_avg = df.groupby('weather_condition').agg({
            'avg_temperature': 'mean',
            'avg_humidity': 'mean',
            'avg_wind_speed': 'mean',
            'avg_severity': 'mean'
        }).reset_index()
        
        fig = go.Figure()
        
        for _, row in weather_avg.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row['avg_temperature']/10, row['avg_humidity']/10, 
                   row['avg_wind_speed'], row['avg_severity']],
                theta=['Temperature', 'Humidity', 'Wind Speed', 'Severity'],
                fill='toself',
                name=row['weather_condition']
            ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 10]
                )),
            showlegend=True,
            title='Weather Conditions Radar Chart',
            height=500
        )
        
        return fig
    
    def create_time_series(self, df: pd.DataFrame) -> go.Figure:
        daily_incidents = df.groupby('date').agg({
            'incident_count': 'sum',
            'avg_severity': 'mean'
        }).reset_index()
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Scatter(
                x=daily_incidents['date'],
                y=daily_incidents['incident_count'],
                name='Incident Count',
                line=dict(color='blue')
            ),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_incidents['date'],
                y=daily_incidents['avg_severity'],
                name='Average Severity',
                line=dict(color='red')
            ),
            secondary_y=True,
        )
        
        fig.update_xaxes(title_text="Date")
        fig.update_yaxes(title_text="Incident Count", secondary_y=False)
        fig.update_yaxes(title_text="Average Severity", secondary_y=True)
        fig.update_layout(title_text="Incidents and Severity Over Time", height=500)
        
        return fig

class MLCharts:
    def __init__(self):
        self.color_palette = px.colors.qualitative.Pastel
    
    def create_model_performance_comparison(self, df: pd.DataFrame) -> go.Figure:
        metrics = ['avg_accuracy', 'avg_precision', 'avg_recall', 'avg_f1']
        
        fig = go.Figure()
        
        for metric in metrics:
            fig.add_trace(go.Bar(
                name=metric.replace('avg_', '').title(),
                x=df['model_name'],
                y=df[metric],
                text=df[metric].round(3),
                textposition='auto'
            ))
        
        fig.update_layout(
            title='Model Performance Comparison',
            xaxis_title='Model Name',
            yaxis_title='Score',
            barmode='group',
            height=500
        )
        
        return fig
    
    def create_prediction_confidence_histogram(self, df: pd.DataFrame) -> go.Figure:
        fig = px.histogram(
            df,
            x='avg_confidence',
            nbins=20,
            title='Prediction Confidence Distribution',
            labels={'avg_confidence': 'Average Confidence Score', 'count': 'Frequency'}
        )
        
        fig.update_layout(height=400)
        
        return fig
    
    def create_feature_importance_chart(self, df: pd.DataFrame, top_n: int = 15) -> go.Figure:
        top_features = df.head(top_n)
        
        fig = px.bar(
            top_features,
            x='importance_score',
            y='feature_name',
            orientation='h',
            title=f'Top {top_n} Feature Importance',
            labels={'importance_score': 'Importance Score', 'feature_name': 'Feature'}
        )
        
        fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
        
        return fig
    
    def create_prediction_accuracy_scatter(self, df: pd.DataFrame) -> go.Figure:
        fig = px.scatter(
            df,
            x='predicted_severity',
            y='actual_severity',
            color='confidence_score',
            size='error',
            title='Predicted vs Actual Severity',
            labels={
                'predicted_severity': 'Predicted Severity',
                'actual_severity': 'Actual Severity',
                'confidence_score': 'Confidence'
            }
        )
        
        fig.add_trace(go.Scatter(
            x=[df['predicted_severity'].min(), df['predicted_severity'].max()],
            y=[df['predicted_severity'].min(), df['predicted_severity'].max()],
            mode='lines',
            name='Perfect Prediction',
            line=dict(dash='dash', color='red')
        ))
        
        fig.update_layout(height=500)
        
        return fig
    
    def create_prediction_timeline(self, df: pd.DataFrame) -> go.Figure:
        daily_predictions = df.groupby(['date', 'predicted_severity']).size().reset_index(name='count')
        
        fig = px.line(
            daily_predictions,
            x='date',
            y='count',
            color='predicted_severity',
            title='Daily Predictions by Severity Level',
            labels={'count': 'Number of Predictions', 'date': 'Date'}
        )
        
        fig.update_layout(height=500)
        
        return fig
    
    def create_model_evolution_chart(self, df: pd.DataFrame) -> go.Figure:
        df_sorted = df.sort_values('training_date')
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        for model in df_sorted['model_name'].unique():
            model_data = df_sorted[df_sorted['model_name'] == model]
            
            fig.add_trace(
                go.Scatter(
                    x=model_data['training_date'],
                    y=model_data['f1_score'],
                    name=f'{model} F1',
                    mode='lines+markers'
                ),
                secondary_y=False,
            )
        
        fig.update_xaxes(title_text="Training Date")
        fig.update_yaxes(title_text="F1 Score", secondary_y=False)
        fig.update_layout(title_text="Model Performance Evolution", height=500)
        
        return fig
    
    def create_confusion_matrix_heatmap(self, df: pd.DataFrame) -> go.Figure:
        confusion_data = pd.crosstab(
            df['actual_severity'].round(),
            df['predicted_severity'].round(),
            normalize='index'
        )
        
        fig = go.Figure(data=go.Heatmap(
            z=confusion_data.values,
            x=confusion_data.columns,
            y=confusion_data.index,
            colorscale='Blues',
            text=confusion_data.values.round(2),
            texttemplate="%{text}",
            textfont={"size": 10}
        ))
        
        fig.update_layout(
            title='Confusion Matrix (Normalized)',
            xaxis_title='Predicted Severity',
            yaxis_title='Actual Severity',
            height=400
        )
        
        return fig
    
    def create_error_distribution(self, df: pd.DataFrame) -> go.Figure:
        fig = px.box(
            df,
            y='error',
            x='predicted_severity',
            title='Prediction Error Distribution by Severity Level',
            labels={'error': 'Prediction Error', 'predicted_severity': 'Predicted Severity'}
        )
        
        fig.update_layout(height=400)
        
        return fig