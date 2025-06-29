from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from decimal import Decimal

class WeatherFrequencyBase(BaseModel):
    weather_category: str = Field(..., max_length=50)
    temperature_category: str = Field(..., max_length=20)
    accident_count: int = Field(..., ge=0)
    total_accidents: int = Field(..., ge=0)
    frequency_percentage: Decimal = Field(..., ge=0, le=100)
    analysis_date: date

class WeatherFrequencyCreate(WeatherFrequencyBase):
    pass

class WeatherFrequencyUpdate(BaseModel):
    weather_category: Optional[str] = Field(None, max_length=50)
    temperature_category: Optional[str] = Field(None, max_length=20)
    accident_count: Optional[int] = Field(None, ge=0)
    total_accidents: Optional[int] = Field(None, ge=0)
    frequency_percentage: Optional[Decimal] = Field(None, ge=0, le=100)
    analysis_date: Optional[date] = None

class WeatherFrequencyResponse(WeatherFrequencyBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class WeatherSeverityBase(BaseModel):
    weather_category: str = Field(..., max_length=50)
    severity_level: int = Field(..., ge=1, le=4)
    accident_count: int = Field(..., ge=0)
    avg_duration_hours: Optional[Decimal] = Field(None, ge=0)
    correlation_score: Optional[Decimal] = Field(None, ge=-1, le=1)
    analysis_date: date

class WeatherSeverityCreate(WeatherSeverityBase):
    pass

class WeatherSeverityUpdate(BaseModel):
    weather_category: Optional[str] = Field(None, max_length=50)
    severity_level: Optional[int] = Field(None, ge=1, le=4)
    accident_count: Optional[int] = Field(None, ge=0)
    avg_duration_hours: Optional[Decimal] = Field(None, ge=0)
    correlation_score: Optional[Decimal] = Field(None, ge=-1, le=1)
    analysis_date: Optional[date] = None

class WeatherSeverityResponse(WeatherSeverityBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class WeatherTrendsBase(BaseModel):
    period_month: str = Field(..., max_length=7, pattern=r'^\d{4}-\d{2}$')
    weather_category: str = Field(..., max_length=50)
    total_accidents: int = Field(..., ge=0)
    avg_severity: Decimal = Field(..., ge=1, le=4)
    trend_direction: Optional[str] = Field(None, max_length=20)
    seasonal_factor: Optional[Decimal] = Field(None, ge=0)
    analysis_date: date

class WeatherTrendsCreate(WeatherTrendsBase):
    pass

class WeatherTrendsUpdate(BaseModel):
    period_month: Optional[str] = Field(None, max_length=7, pattern=r'^\d{4}-\d{2}$')
    weather_category: Optional[str] = Field(None, max_length=50)
    total_accidents: Optional[int] = Field(None, ge=0)
    avg_severity: Optional[Decimal] = Field(None, ge=1, le=4)
    trend_direction: Optional[str] = Field(None, max_length=20)
    seasonal_factor: Optional[Decimal] = Field(None, ge=0)
    analysis_date: Optional[date] = None

class WeatherTrendsResponse(WeatherTrendsBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class PaginationParams(BaseModel):
    skip: int = Field(0, ge=0)
    limit: int = Field(100, ge=1, le=1000)

class FilterParams(BaseModel):
    weather_category: Optional[str] = None
    analysis_date_from: Optional[date] = None
    analysis_date_to: Optional[date] = None

class AnalyticsSummary(BaseModel):
    total_records: int
    weather_categories: list[str]
    date_range: dict[str, Optional[date]]
    avg_accident_count: Decimal
    most_common_weather: str

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    database_connected: bool
    version: str = "1.0.0"