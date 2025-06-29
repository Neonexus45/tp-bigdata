from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Optional
from datetime import date
from . import models
from .database import WeatherAccidentFrequency, WeatherSeverityCorrelation, WeatherImpactTrends

class WeatherFrequencyCRUD:
    @staticmethod
    def create(db: Session, item: models.WeatherFrequencyCreate) -> WeatherAccidentFrequency:
        db_item = WeatherAccidentFrequency(**item.dict())
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def get(db: Session, item_id: int) -> Optional[WeatherAccidentFrequency]:
        return db.query(WeatherAccidentFrequency).filter(WeatherAccidentFrequency.id == item_id).first()
    
    @staticmethod
    def get_multi(
        db: Session, 
        skip: int = 0, 
        limit: int = 100,
        weather_category: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None
    ) -> List[WeatherAccidentFrequency]:
        query = db.query(WeatherAccidentFrequency)
        
        if weather_category:
            query = query.filter(WeatherAccidentFrequency.weather_category == weather_category)
        if analysis_date_from:
            query = query.filter(WeatherAccidentFrequency.analysis_date >= analysis_date_from)
        if analysis_date_to:
            query = query.filter(WeatherAccidentFrequency.analysis_date <= analysis_date_to)
        
        return query.offset(skip).limit(limit).all()
    
    @staticmethod
    def update(db: Session, item_id: int, item_update: models.WeatherFrequencyUpdate) -> Optional[WeatherAccidentFrequency]:
        db_item = db.query(WeatherAccidentFrequency).filter(WeatherAccidentFrequency.id == item_id).first()
        if not db_item:
            return None
        
        update_data = item_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_item, field, value)
        
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def delete(db: Session, item_id: int) -> bool:
        db_item = db.query(WeatherAccidentFrequency).filter(WeatherAccidentFrequency.id == item_id).first()
        if not db_item:
            return False
        
        db.delete(db_item)
        db.commit()
        return True
    
    @staticmethod
    def count(db: Session) -> int:
        return db.query(WeatherAccidentFrequency).count()

class WeatherSeverityCRUD:
    @staticmethod
    def create(db: Session, item: models.WeatherSeverityCreate) -> WeatherSeverityCorrelation:
        db_item = WeatherSeverityCorrelation(**item.dict())
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def get(db: Session, item_id: int) -> Optional[WeatherSeverityCorrelation]:
        return db.query(WeatherSeverityCorrelation).filter(WeatherSeverityCorrelation.id == item_id).first()
    
    @staticmethod
    def get_multi(
        db: Session, 
        skip: int = 0, 
        limit: int = 100,
        weather_category: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None
    ) -> List[WeatherSeverityCorrelation]:
        query = db.query(WeatherSeverityCorrelation)
        
        if weather_category:
            query = query.filter(WeatherSeverityCorrelation.weather_category == weather_category)
        if analysis_date_from:
            query = query.filter(WeatherSeverityCorrelation.analysis_date >= analysis_date_from)
        if analysis_date_to:
            query = query.filter(WeatherSeverityCorrelation.analysis_date <= analysis_date_to)
        
        return query.offset(skip).limit(limit).all()
    
    @staticmethod
    def update(db: Session, item_id: int, item_update: models.WeatherSeverityUpdate) -> Optional[WeatherSeverityCorrelation]:
        db_item = db.query(WeatherSeverityCorrelation).filter(WeatherSeverityCorrelation.id == item_id).first()
        if not db_item:
            return None
        
        update_data = item_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_item, field, value)
        
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def delete(db: Session, item_id: int) -> bool:
        db_item = db.query(WeatherSeverityCorrelation).filter(WeatherSeverityCorrelation.id == item_id).first()
        if not db_item:
            return False
        
        db.delete(db_item)
        db.commit()
        return True
    
    @staticmethod
    def count(db: Session) -> int:
        return db.query(WeatherSeverityCorrelation).count()

class WeatherTrendsCRUD:
    @staticmethod
    def create(db: Session, item: models.WeatherTrendsCreate) -> WeatherImpactTrends:
        db_item = WeatherImpactTrends(**item.dict())
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def get(db: Session, item_id: int) -> Optional[WeatherImpactTrends]:
        return db.query(WeatherImpactTrends).filter(WeatherImpactTrends.id == item_id).first()
    
    @staticmethod
    def get_multi(
        db: Session, 
        skip: int = 0, 
        limit: int = 100,
        weather_category: Optional[str] = None,
        analysis_date_from: Optional[date] = None,
        analysis_date_to: Optional[date] = None
    ) -> List[WeatherImpactTrends]:
        query = db.query(WeatherImpactTrends)
        
        if weather_category:
            query = query.filter(WeatherImpactTrends.weather_category == weather_category)
        if analysis_date_from:
            query = query.filter(WeatherImpactTrends.analysis_date >= analysis_date_from)
        if analysis_date_to:
            query = query.filter(WeatherImpactTrends.analysis_date <= analysis_date_to)
        
        return query.offset(skip).limit(limit).all()
    
    @staticmethod
    def update(db: Session, item_id: int, item_update: models.WeatherTrendsUpdate) -> Optional[WeatherImpactTrends]:
        db_item = db.query(WeatherImpactTrends).filter(WeatherImpactTrends.id == item_id).first()
        if not db_item:
            return None
        
        update_data = item_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_item, field, value)
        
        db.commit()
        db.refresh(db_item)
        return db_item
    
    @staticmethod
    def delete(db: Session, item_id: int) -> bool:
        db_item = db.query(WeatherImpactTrends).filter(WeatherImpactTrends.id == item_id).first()
        if not db_item:
            return False
        
        db.delete(db_item)
        db.commit()
        return True
    
    @staticmethod
    def count(db: Session) -> int:
        return db.query(WeatherImpactTrends).count()

class AnalyticsCRUD:
    @staticmethod
    def get_summary(db: Session) -> models.AnalyticsSummary:
        total_records = (
            WeatherFrequencyCRUD.count(db) + 
            WeatherSeverityCRUD.count(db) + 
            WeatherTrendsCRUD.count(db)
        )
        
        weather_categories = db.query(WeatherAccidentFrequency.weather_category).distinct().all()
        weather_categories = [cat[0] for cat in weather_categories]
        
        date_range_query = db.query(
            func.min(WeatherAccidentFrequency.analysis_date).label('min_date'),
            func.max(WeatherAccidentFrequency.analysis_date).label('max_date')
        ).first()
        
        date_range = {
            "from": date_range_query.min_date,
            "to": date_range_query.max_date
        }
        
        avg_accident_count = db.query(func.avg(WeatherAccidentFrequency.accident_count)).scalar() or 0
        
        most_common_weather = db.query(
            WeatherAccidentFrequency.weather_category,
            func.count(WeatherAccidentFrequency.weather_category).label('count')
        ).group_by(WeatherAccidentFrequency.weather_category).order_by(func.count(WeatherAccidentFrequency.weather_category).desc()).first()
        
        most_common_weather = most_common_weather[0] if most_common_weather else "Unknown"
        
        return models.AnalyticsSummary(
            total_records=total_records,
            weather_categories=weather_categories,
            date_range=date_range,
            avg_accident_count=avg_accident_count,
            most_common_weather=most_common_weather
        )