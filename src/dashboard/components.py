import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import plotly.graph_objects as go

class MetricsDisplay:
    def __init__(self):
        self.default_style = {
            'background-color': '#f0f2f6',
            'padding': '10px',
            'border-radius': '5px',
            'margin': '5px'
        }
    
    def show_kpi_cards(self, metrics: Dict[str, Any], columns: int = 4):
        cols = st.columns(columns)
        
        for i, (key, value) in enumerate(metrics.items()):
            with cols[i % columns]:
                if isinstance(value, dict):
                    self._render_metric_card(
                        key, 
                        value.get('value', 0), 
                        value.get('delta', None),
                        value.get('help', None)
                    )
                else:
                    self._render_metric_card(key, value)
    
    def _render_metric_card(self, label: str, value: Any, delta: Any = None, help_text: str = None):
        if isinstance(value, float):
            if value < 1:
                formatted_value = f"{value:.3f}"
            else:
                formatted_value = f"{value:.2f}"
        elif isinstance(value, int):
            formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)
        
        st.metric(
            label=label.replace('_', ' ').title(),
            value=formatted_value,
            delta=delta,
            help=help_text
        )
    
    def show_summary_stats(self, df: pd.DataFrame, title: str = "Summary Statistics"):
        st.subheader(title)
        
        if df.empty:
            st.warning("No data available for summary statistics")
            return
        
        numeric_cols = df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) > 0:
            summary_stats = df[numeric_cols].describe()
            st.dataframe(summary_stats, use_container_width=True)
        else:
            st.info("No numeric columns found for summary statistics")
    
    def show_data_quality_metrics(self, df: pd.DataFrame):
        if df.empty:
            st.warning("No data available for quality metrics")
            return
        
        total_rows = len(df)
        total_cols = len(df.columns)
        missing_values = df.isnull().sum().sum()
        missing_percentage = (missing_values / (total_rows * total_cols)) * 100
        
        quality_metrics = {
            'Total Rows': total_rows,
            'Total Columns': total_cols,
            'Missing Values': missing_values,
            'Data Completeness': f"{100 - missing_percentage:.1f}%"
        }
        
        self.show_kpi_cards(quality_metrics)

class FilterPanel:
    def __init__(self):
        self.filters = {}
    
    def create_date_range_filter(self, 
                                key: str = "date_range",
                                default_days: int = 30,
                                label: str = "Select Date Range") -> Tuple[datetime, datetime]:
        
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input(
                f"Start Date ({key})",
                value=datetime.now() - timedelta(days=default_days),
                key=f"{key}_start"
            )
        
        with col2:
            end_date = st.date_input(
                f"End Date ({key})",
                value=datetime.now(),
                key=f"{key}_end"
            )
        
        return start_date, end_date
    
    def create_multiselect_filter(self, 
                                 options: List[str],
                                 key: str,
                                 label: str,
                                 default: Optional[List[str]] = None) -> List[str]:
        
        if default is None:
            default = options[:min(5, len(options))]
        
        selected = st.multiselect(
            label,
            options=options,
            default=default,
            key=key
        )
        
        return selected if selected else options
    
    def create_slider_filter(self,
                           min_val: float,
                           max_val: float,
                           key: str,
                           label: str,
                           step: float = 0.1) -> Tuple[float, float]:
        
        return st.slider(
            label,
            min_value=min_val,
            max_value=max_val,
            value=(min_val, max_val),
            step=step,
            key=key
        )
    
    def create_selectbox_filter(self,
                              options: List[str],
                              key: str,
                              label: str,
                              default_index: int = 0) -> str:
        
        return st.selectbox(
            label,
            options=options,
            index=default_index,
            key=key
        )
    
    def create_weather_condition_filter(self, df: pd.DataFrame) -> List[str]:
        if 'weather_condition' in df.columns:
            unique_conditions = df['weather_condition'].unique().tolist()
            return self.create_multiselect_filter(
                options=unique_conditions,
                key="weather_conditions",
                label="Select Weather Conditions"
            )
        return []
    
    def create_severity_filter(self, df: pd.DataFrame) -> Tuple[float, float]:
        if 'severity_score' in df.columns:
            min_severity = float(df['severity_score'].min())
            max_severity = float(df['severity_score'].max())
            return self.create_slider_filter(
                min_val=min_severity,
                max_val=max_severity,
                key="severity_range",
                label="Severity Score Range",
                step=0.1
            )
        return 0.0, 10.0
    
    def create_model_filter(self, df: pd.DataFrame) -> str:
        if 'model_name' in df.columns:
            unique_models = df['model_name'].unique().tolist()
            return self.create_selectbox_filter(
                options=['All'] + unique_models,
                key="selected_model",
                label="Select Model"
            )
        return 'All'
    
    def apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        filtered_df = df.copy()
        
        for filter_name, filter_value in filters.items():
            if filter_value is None:
                continue
                
            if filter_name == 'date_range' and len(filter_value) == 2:
                start_date, end_date = filter_value
                if 'date' in filtered_df.columns:
                    filtered_df = filtered_df[
                        (pd.to_datetime(filtered_df['date']) >= pd.to_datetime(start_date)) &
                        (pd.to_datetime(filtered_df['date']) <= pd.to_datetime(end_date))
                    ]
            
            elif filter_name == 'weather_conditions' and filter_value:
                if 'weather_condition' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['weather_condition'].isin(filter_value)]
            
            elif filter_name == 'severity_range' and len(filter_value) == 2:
                min_sev, max_sev = filter_value
                if 'severity_score' in filtered_df.columns:
                    filtered_df = filtered_df[
                        (filtered_df['severity_score'] >= min_sev) &
                        (filtered_df['severity_score'] <= max_sev)
                    ]
            
            elif filter_name == 'selected_model' and filter_value != 'All':
                if 'model_name' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['model_name'] == filter_value]
        
        return filtered_df

class DataTable:
    def __init__(self):
        self.default_config = {
            'use_container_width': True,
            'hide_index': True
        }
    
    def show_dataframe(self, 
                      df: pd.DataFrame, 
                      title: str = None,
                      max_rows: int = 100,
                      columns_to_show: List[str] = None,
                      format_dict: Dict[str, str] = None):
        
        if title:
            st.subheader(title)
        
        if df.empty:
            st.warning("No data to display")
            return
        
        display_df = df.copy()
        
        if columns_to_show:
            available_cols = [col for col in columns_to_show if col in display_df.columns]
            if available_cols:
                display_df = display_df[available_cols]
        
        if len(display_df) > max_rows:
            st.info(f"Showing first {max_rows} rows out of {len(display_df)} total rows")
            display_df = display_df.head(max_rows)
        
        if format_dict:
            for col, format_str in format_dict.items():
                if col in display_df.columns:
                    if display_df[col].dtype in ['float64', 'float32']:
                        display_df[col] = display_df[col].apply(lambda x: format_str.format(x) if pd.notna(x) else '')
        
        st.dataframe(display_df, **self.default_config)
    
    def show_paginated_table(self, 
                           df: pd.DataFrame, 
                           page_size: int = 20,
                           title: str = None):
        
        if title:
            st.subheader(title)
        
        if df.empty:
            st.warning("No data to display")
            return
        
        total_rows = len(df)
        total_pages = (total_rows - 1) // page_size + 1
        
        if total_pages > 1:
            page = st.selectbox(
                f"Page (Total: {total_pages})",
                range(1, total_pages + 1),
                key=f"page_{title}"
            )
            
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_rows)
            
            st.info(f"Showing rows {start_idx + 1} to {end_idx} of {total_rows}")
            display_df = df.iloc[start_idx:end_idx]
        else:
            display_df = df
        
        st.dataframe(display_df, **self.default_config)
    
    def show_sortable_table(self, 
                          df: pd.DataFrame,
                          title: str = None,
                          sort_column: str = None,
                          ascending: bool = True):
        
        if title:
            st.subheader(title)
        
        if df.empty:
            st.warning("No data to display")
            return
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            sort_col = st.selectbox(
                "Sort by column",
                options=df.columns.tolist(),
                index=df.columns.tolist().index(sort_column) if sort_column in df.columns else 0,
                key=f"sort_col_{title}"
            )
        
        with col2:
            sort_order = st.selectbox(
                "Sort order",
                options=['Ascending', 'Descending'],
                index=0 if ascending else 1,
                key=f"sort_order_{title}"
            )
        
        with col3:
            max_rows = st.number_input(
                "Max rows",
                min_value=10,
                max_value=1000,
                value=100,
                step=10,
                key=f"max_rows_{title}"
            )
        
        sorted_df = df.sort_values(
            by=sort_col,
            ascending=(sort_order == 'Ascending')
        ).head(max_rows)
        
        st.dataframe(sorted_df, **self.default_config)
    
    def show_downloadable_table(self, 
                              df: pd.DataFrame,
                              filename: str = "data.csv",
                              title: str = None):
        
        if title:
            st.subheader(title)
        
        if df.empty:
            st.warning("No data to display")
            return
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.dataframe(df, **self.default_config)
        
        with col2:
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=filename,
                mime="text/csv"
            )