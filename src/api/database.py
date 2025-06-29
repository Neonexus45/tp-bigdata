import os
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, Date, TIMESTAMP, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class WeatherAccidentFrequency(Base):
    __tablename__ = "weather_accident_frequency"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    weather_category = Column(String(50), nullable=False)
    temperature_category = Column(String(20), nullable=False)
    accident_count = Column(Integer, nullable=False)
    total_accidents = Column(Integer, nullable=False)
    frequency_percentage = Column(DECIMAL(5,2), nullable=False)
    analysis_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

class WeatherSeverityCorrelation(Base):
    __tablename__ = "weather_severity_correlation"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    weather_category = Column(String(50), nullable=False)
    severity_level = Column(Integer, nullable=False)
    accident_count = Column(Integer, nullable=False)
    avg_duration_hours = Column(DECIMAL(8,2))
    correlation_score = Column(DECIMAL(5,3))
    analysis_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

class WeatherImpactTrends(Base):
    __tablename__ = "weather_impact_trends"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    period_month = Column(String(7), nullable=False)
    weather_category = Column(String(50), nullable=False)
    total_accidents = Column(Integer, nullable=False)
    avg_severity = Column(DECIMAL(3,2), nullable=False)
    trend_direction = Column(String(20))
    seasonal_factor = Column(DECIMAL(5,3))
    analysis_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

def get_database_url() -> str:
    host = os.getenv("MYSQL_ANALYTICS_HOST")
    port = os.getenv("MYSQL_ANALYTICS_PORT")
    database = os.getenv("MYSQL_ANALYTICS_DATABASE")
    user = os.getenv("MYSQL_ANALYTICS_USER")
    password = os.getenv("MYSQL_ANALYTICS_PASSWORD")
    
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(get_database_url(), pool_pre_ping=True, pool_recycle=300)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    Base.metadata.create_all(bind=engine)