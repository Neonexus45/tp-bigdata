import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

# Handle imports with proper error handling
try:
    from data_loader import AnalyticsDataLoader, MLDataLoader, DataRefreshUtility
    from visualizations import WeatherCharts, MLCharts
    from components import MetricsDisplay, FilterPanel, DataTable
    IMPORTS_SUCCESSFUL = True
except ImportError as e:
    st.error(f"Import error: {e}")
    IMPORTS_SUCCESSFUL = False

st.set_page_config(
    page_title="Weather Analytics & ML Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

class DashboardApp:
    def __init__(self):
        if not IMPORTS_SUCCESSFUL:
            st.error("Cannot initialize dashboard due to import errors")
            return
            
        try:
            self.analytics_loader = AnalyticsDataLoader()
            self.ml_loader = MLDataLoader()
            self.weather_charts = WeatherCharts()
            self.ml_charts = MLCharts()
            self.metrics_display = MetricsDisplay()
            self.filter_panel = FilterPanel()
            self.data_table = DataTable()
            self.initialized = True
        except Exception as e:
            st.error(f"Dashboard initialization error: {e}")
            self.initialized = False
        
        if 'page' not in st.session_state:
            st.session_state.page = 'Home'
    
    def run(self):
        if not IMPORTS_SUCCESSFUL or not hasattr(self, 'initialized') or not self.initialized:
            st.title("⚠️ Dashboard Error")
            st.error("Dashboard could not be initialized properly.")
            st.info("Please check the database connections and configuration.")
            return
            
        self.setup_sidebar()
        self.render_main_content()
    
    def setup_sidebar(self):
        st.sidebar.title("🌤️ Weather Analytics Dashboard")
        
        pages = {
            'Home': '🏠',
            'Weather Analytics': '🌦️',
            'ML Predictions': '🤖',
            'Data Explorer': '📊'
        }
        
        selected_page = st.sidebar.selectbox(
            "Navigate to:",
            options=list(pages.keys()),
            format_func=lambda x: f"{pages[x]} {x}",
            key="page_selector"
        )
        
        st.session_state.page = selected_page
        
        st.sidebar.markdown("---")
        
        st.sidebar.subheader("Data Controls")
        if st.sidebar.button("🔄 Refresh Data"):
            DataRefreshUtility.clear_cache()
            DataRefreshUtility.update_refresh_time()
        
        st.sidebar.info(f"Last refresh: {DataRefreshUtility.get_last_refresh_time()}")
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### About")
        st.sidebar.markdown(
            "This dashboard provides insights into weather impact analysis "
            "and machine learning predictions for severity assessment."
        )
    
    def render_main_content(self):
        if st.session_state.page == 'Home':
            self.render_home_page()
        elif st.session_state.page == 'Weather Analytics':
            self.render_weather_analytics_page()
        elif st.session_state.page == 'ML Predictions':
            self.render_ml_predictions_page()
        elif st.session_state.page == 'Data Explorer':
            self.render_data_explorer_page()
    
    def render_home_page(self):
        st.title("🏠 Dashboard Overview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Weather Analytics Summary")
            weather_summary = self.analytics_loader.get_weather_impact_summary()
            
            if not weather_summary.empty:
                total_incidents = weather_summary['incident_count'].sum()
                avg_severity = weather_summary['avg_severity'].mean()
                top_weather = weather_summary.iloc[0]['weather_condition']
                
                weather_metrics = {
                    'Total Incidents': total_incidents,
                    'Average Severity': f"{avg_severity:.2f}",
                    'Top Weather Impact': top_weather,
                    'Weather Conditions': len(weather_summary)
                }
                
                self.metrics_display.show_kpi_cards(weather_metrics, columns=2)
                
                st.plotly_chart(
                    self.weather_charts.create_weather_impact_bar_chart(weather_summary),
                    use_container_width=True
                )
            else:
                st.warning("No weather analytics data available")
        
        with col2:
            st.subheader("ML Performance Summary")
            model_performance = self.ml_loader.get_model_performance()
            
            if not model_performance.empty:
                active_models = model_performance[model_performance['is_active'] == 1]
                best_model = model_performance.loc[model_performance['f1_score'].idxmax()]
                
                ml_metrics = {
                    'Active Models': len(active_models),
                    'Best F1 Score': f"{best_model['f1_score']:.3f}",
                    'Best Model': best_model['model_name'],
                    'Total Versions': len(model_performance)
                }
                
                self.metrics_display.show_kpi_cards(ml_metrics, columns=2)
                
                model_comparison = self.ml_loader.get_model_comparison()
                if not model_comparison.empty:
                    st.plotly_chart(
                        self.ml_charts.create_model_performance_comparison(model_comparison),
                        use_container_width=True
                    )
            else:
                st.warning("No ML performance data available")
        
        st.markdown("---")
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Recent Predictions")
            recent_predictions = self.ml_loader.get_predictions_summary(days=7)
            if not recent_predictions.empty:
                self.data_table.show_dataframe(
                    recent_predictions.head(10),
                    max_rows=10,
                    format_dict={'avg_confidence': '{:.3f}'}
                )
            else:
                st.info("No recent predictions available")
        
        with col4:
            st.subheader("Weather Trends")
            temporal_data = self.analytics_loader.get_temporal_analysis()
            if not temporal_data.empty:
                recent_trends = temporal_data.head(10)
                self.data_table.show_dataframe(
                    recent_trends,
                    max_rows=10,
                    format_dict={'avg_severity': '{:.2f}'}
                )
            else:
                st.info("No temporal data available")
    
    def render_weather_analytics_page(self):
        st.title("🌦️ Weather Analytics")
        
        st.sidebar.subheader("Filters")
        start_date, end_date = self.filter_panel.create_date_range_filter(
            key="weather_date_range",
            default_days=30
        )
        
        temporal_data = self.analytics_loader.get_temporal_analysis(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        weather_conditions = self.filter_panel.create_weather_condition_filter(temporal_data)
        severity_range = self.filter_panel.create_severity_filter(temporal_data)
        
        filters = {
            'date_range': (start_date, end_date),
            'weather_conditions': weather_conditions,
            'severity_range': severity_range
        }
        
        filtered_data = self.filter_panel.apply_filters(temporal_data, filters)
        
        if not filtered_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Weather Impact Overview")
                weather_summary = self.analytics_loader.get_weather_impact_summary()
                if not weather_summary.empty:
                    st.plotly_chart(
                        self.weather_charts.create_weather_impact_bar_chart(weather_summary),
                        use_container_width=True
                    )
            
            with col2:
                st.subheader("Severity Distribution")
                severity_dist = self.analytics_loader.get_severity_distribution()
                if not severity_dist.empty:
                    st.plotly_chart(
                        self.weather_charts.create_severity_distribution_pie(severity_dist),
                        use_container_width=True
                    )
            
            st.subheader("Temporal Analysis")
            st.plotly_chart(
                self.weather_charts.create_time_series(filtered_data),
                use_container_width=True
            )
            
            st.subheader("Weather Patterns Heatmap")
            st.plotly_chart(
                self.weather_charts.create_temporal_heatmap(filtered_data),
                use_container_width=True
            )
            
            col3, col4 = st.columns(2)
            
            with col3:
                st.subheader("Weather Metrics Radar")
                weather_summary = self.analytics_loader.get_weather_impact_summary()
                if not weather_summary.empty:
                    st.plotly_chart(
                        self.weather_charts.create_weather_metrics_radar(weather_summary),
                        use_container_width=True
                    )
            
            with col4:
                st.subheader("Correlation Analysis")
                correlation_data = self.analytics_loader.get_correlation_data()
                if not correlation_data.empty:
                    x_axis = st.selectbox("X-axis", ['temperature', 'humidity', 'wind_speed', 'pressure'])
                    y_axis = st.selectbox("Y-axis", ['severity_score', 'temperature', 'humidity', 'wind_speed'])
                    
                    st.plotly_chart(
                        self.weather_charts.create_correlation_scatter(correlation_data, x_axis, y_axis),
                        use_container_width=True
                    )
            
            st.subheader("Detailed Data")
            self.data_table.show_sortable_table(
                filtered_data,
                title="Weather Analytics Data",
                sort_column="date"
            )
        else:
            st.warning("No data available for the selected filters")
    
    def render_ml_predictions_page(self):
        st.title("🤖 ML Predictions")
        
        st.sidebar.subheader("ML Filters")
        
        model_performance = self.ml_loader.get_model_performance()
        selected_model = self.filter_panel.create_model_filter(model_performance)
        
        prediction_days = st.sidebar.slider(
            "Prediction History (days)",
            min_value=7,
            max_value=90,
            value=30,
            key="prediction_days"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Model Performance")
            if not model_performance.empty:
                filtered_models = model_performance if selected_model == 'All' else model_performance[model_performance['model_name'] == selected_model]
                
                model_comparison = self.ml_loader.get_model_comparison()
                if not model_comparison.empty:
                    st.plotly_chart(
                        self.ml_charts.create_model_performance_comparison(model_comparison),
                        use_container_width=True
                    )
                
                self.metrics_display.show_summary_stats(filtered_models, "Model Performance Stats")
        
        with col2:
            st.subheader("Prediction Confidence")
            predictions_summary = self.ml_loader.get_predictions_summary(days=prediction_days)
            if not predictions_summary.empty:
                st.plotly_chart(
                    self.ml_charts.create_prediction_confidence_histogram(predictions_summary),
                    use_container_width=True
                )
        
        st.subheader("Feature Importance")
        feature_importance = self.ml_loader.get_feature_importance(
            model_name=None if selected_model == 'All' else selected_model
        )
        if not feature_importance.empty:
            top_n = st.slider("Number of top features", 5, 25, 15, key="top_features")
            st.plotly_chart(
                self.ml_charts.create_feature_importance_chart(feature_importance, top_n),
                use_container_width=True
            )
        
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Prediction Timeline")
            if not predictions_summary.empty:
                st.plotly_chart(
                    self.ml_charts.create_prediction_timeline(predictions_summary),
                    use_container_width=True
                )
        
        with col4:
            st.subheader("Model Evolution")
            if not model_performance.empty:
                st.plotly_chart(
                    self.ml_charts.create_model_evolution_chart(model_performance),
                    use_container_width=True
                )
        
        st.subheader("Prediction Accuracy Analysis")
        prediction_accuracy = self.ml_loader.get_prediction_accuracy()
        if not prediction_accuracy.empty:
            col5, col6 = st.columns(2)
            
            with col5:
                st.plotly_chart(
                    self.ml_charts.create_prediction_accuracy_scatter(prediction_accuracy),
                    use_container_width=True
                )
            
            with col6:
                st.plotly_chart(
                    self.ml_charts.create_confusion_matrix_heatmap(prediction_accuracy),
                    use_container_width=True
                )
            
            st.plotly_chart(
                self.ml_charts.create_error_distribution(prediction_accuracy),
                use_container_width=True
            )
        
        st.subheader("Model Performance Details")
        self.data_table.show_downloadable_table(
            model_performance,
            filename="model_performance.csv",
            title="Model Performance Data"
        )
    
    def render_data_explorer_page(self):
        st.title("📊 Data Explorer")
        
        data_source = st.selectbox(
            "Select Data Source",
            ["Weather Analytics", "ML Predictions", "Model Performance"],
            key="data_source"
        )
        
        if data_source == "Weather Analytics":
            st.subheader("Weather Analytics Data")
            
            data_type = st.selectbox(
                "Select Data Type",
                ["Weather Impact Summary", "Temporal Analysis", "Severity Distribution", "Correlation Data"],
                key="weather_data_type"
            )
            
            if data_type == "Weather Impact Summary":
                data = self.analytics_loader.get_weather_impact_summary()
            elif data_type == "Temporal Analysis":
                data = self.analytics_loader.get_temporal_analysis()
            elif data_type == "Severity Distribution":
                data = self.analytics_loader.get_severity_distribution()
            else:
                data = self.analytics_loader.get_correlation_data()
            
        elif data_source == "ML Predictions":
            st.subheader("ML Predictions Data")
            
            data_type = st.selectbox(
                "Select Data Type",
                ["Predictions Summary", "Feature Importance", "Prediction Accuracy"],
                key="ml_data_type"
            )
            
            if data_type == "Predictions Summary":
                days = st.slider("Days", 7, 90, 30, key="pred_days")
                data = self.ml_loader.get_predictions_summary(days)
            elif data_type == "Feature Importance":
                data = self.ml_loader.get_feature_importance()
            else:
                data = self.ml_loader.get_prediction_accuracy()
        
        else:
            st.subheader("Model Performance Data")
            data_type = st.selectbox(
                "Select Data Type",
                ["Model Performance", "Model Comparison"],
                key="model_data_type"
            )
            
            if data_type == "Model Performance":
                data = self.ml_loader.get_model_performance()
            else:
                data = self.ml_loader.get_model_comparison()
        
        if not data.empty:
            col1, col2 = st.columns([3, 1])
            
            with col1:
                self.data_table.show_paginated_table(data, page_size=25, title=f"{data_source} - {data_type}")
            
            with col2:
                self.metrics_display.show_data_quality_metrics(data)
                
                st.subheader("Data Info")
                st.write(f"**Rows:** {len(data)}")
                st.write(f"**Columns:** {len(data.columns)}")
                st.write(f"**Memory Usage:** {data.memory_usage(deep=True).sum() / 1024:.1f} KB")
            
            st.subheader("Column Statistics")
            numeric_cols = data.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                selected_cols = st.multiselect(
                    "Select columns for detailed statistics",
                    numeric_cols.tolist(),
                    default=numeric_cols.tolist()[:5]
                )
                
                if selected_cols:
                    st.dataframe(data[selected_cols].describe(), use_container_width=True)
            
            st.subheader("Raw Data Export")
            csv = data.to_csv(index=False)
            st.download_button(
                label="Download Full Dataset as CSV",
                data=csv,
                file_name=f"{data_source.lower().replace(' ', '_')}_{data_type.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )
        else:
            st.warning(f"No data available for {data_source} - {data_type}")

def main():
    app = DashboardApp()
    app.run()

if __name__ == "__main__":
    main()