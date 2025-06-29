from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, datetime
from . import models, crud
from .database import get_db

router = APIRouter()

@router.get("/health", response_model=models.HealthCheck)
async def health_check(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        database_connected = True
    except Exception:
        database_connected = False
    
    return models.HealthCheck(
        status="healthy" if database_connected else "unhealthy",
        timestamp=datetime.now(),
        database_connected=database_connected
    )

@router.get("/weather-frequency/", response_model=List[models.WeatherFrequencyResponse])
async def get_weather_frequency(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    weather_category: Optional[str] = Query(None),
    analysis_date_from: Optional[date] = Query(None),
    analysis_date_to: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    items = crud.WeatherFrequencyCRUD.get_multi(
        db=db, 
        skip=skip, 
        limit=limit,
        weather_category=weather_category,
        analysis_date_from=analysis_date_from,
        analysis_date_to=analysis_date_to
    )
    return items

@router.get("/weather-frequency/{item_id}", response_model=models.WeatherFrequencyResponse)
async def get_weather_frequency_by_id(item_id: int, db: Session = Depends(get_db)):
    item = crud.WeatherFrequencyCRUD.get(db=db, item_id=item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Weather frequency record not found")
    return item

@router.post("/weather-frequency/", response_model=models.WeatherFrequencyResponse, status_code=201)
async def create_weather_frequency(item: models.WeatherFrequencyCreate, db: Session = Depends(get_db)):
    return crud.WeatherFrequencyCRUD.create(db=db, item=item)

@router.put("/weather-frequency/{item_id}", response_model=models.WeatherFrequencyResponse)
async def update_weather_frequency(item_id: int, item_update: models.WeatherFrequencyUpdate, db: Session = Depends(get_db)):
    item = crud.WeatherFrequencyCRUD.update(db=db, item_id=item_id, item_update=item_update)
    if not item:
        raise HTTPException(status_code=404, detail="Weather frequency record not found")
    return item

@router.delete("/weather-frequency/{item_id}", status_code=204)
async def delete_weather_frequency(item_id: int, db: Session = Depends(get_db)):
    success = crud.WeatherFrequencyCRUD.delete(db=db, item_id=item_id)
    if not success:
        raise HTTPException(status_code=404, detail="Weather frequency record not found")

@router.get("/weather-severity/", response_model=List[models.WeatherSeverityResponse])
async def get_weather_severity(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    weather_category: Optional[str] = Query(None),
    analysis_date_from: Optional[date] = Query(None),
    analysis_date_to: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    items = crud.WeatherSeverityCRUD.get_multi(
        db=db, 
        skip=skip, 
        limit=limit,
        weather_category=weather_category,
        analysis_date_from=analysis_date_from,
        analysis_date_to=analysis_date_to
    )
    return items

@router.get("/weather-severity/{item_id}", response_model=models.WeatherSeverityResponse)
async def get_weather_severity_by_id(item_id: int, db: Session = Depends(get_db)):
    item = crud.WeatherSeverityCRUD.get(db=db, item_id=item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Weather severity record not found")
    return item

@router.post("/weather-severity/", response_model=models.WeatherSeverityResponse, status_code=201)
async def create_weather_severity(item: models.WeatherSeverityCreate, db: Session = Depends(get_db)):
    return crud.WeatherSeverityCRUD.create(db=db, item=item)

@router.put("/weather-severity/{item_id}", response_model=models.WeatherSeverityResponse)
async def update_weather_severity(item_id: int, item_update: models.WeatherSeverityUpdate, db: Session = Depends(get_db)):
    item = crud.WeatherSeverityCRUD.update(db=db, item_id=item_id, item_update=item_update)
    if not item:
        raise HTTPException(status_code=404, detail="Weather severity record not found")
    return item

@router.delete("/weather-severity/{item_id}", status_code=204)
async def delete_weather_severity(item_id: int, db: Session = Depends(get_db)):
    success = crud.WeatherSeverityCRUD.delete(db=db, item_id=item_id)
    if not success:
        raise HTTPException(status_code=404, detail="Weather severity record not found")

@router.get("/weather-trends/", response_model=List[models.WeatherTrendsResponse])
async def get_weather_trends(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    weather_category: Optional[str] = Query(None),
    analysis_date_from: Optional[date] = Query(None),
    analysis_date_to: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    items = crud.WeatherTrendsCRUD.get_multi(
        db=db, 
        skip=skip, 
        limit=limit,
        weather_category=weather_category,
        analysis_date_from=analysis_date_from,
        analysis_date_to=analysis_date_to
    )
    return items

@router.get("/weather-trends/{item_id}", response_model=models.WeatherTrendsResponse)
async def get_weather_trends_by_id(item_id: int, db: Session = Depends(get_db)):
    item = crud.WeatherTrendsCRUD.get(db=db, item_id=item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Weather trends record not found")
    return item

@router.post("/weather-trends/", response_model=models.WeatherTrendsResponse, status_code=201)
async def create_weather_trends(item: models.WeatherTrendsCreate, db: Session = Depends(get_db)):
    return crud.WeatherTrendsCRUD.create(db=db, item=item)

@router.put("/weather-trends/{item_id}", response_model=models.WeatherTrendsResponse)
async def update_weather_trends(item_id: int, item_update: models.WeatherTrendsUpdate, db: Session = Depends(get_db)):
    item = crud.WeatherTrendsCRUD.update(db=db, item_id=item_id, item_update=item_update)
    if not item:
        raise HTTPException(status_code=404, detail="Weather trends record not found")
    return item

@router.delete("/weather-trends/{item_id}", status_code=204)
async def delete_weather_trends(item_id: int, db: Session = Depends(get_db)):
    success = crud.WeatherTrendsCRUD.delete(db=db, item_id=item_id)
    if not success:
        raise HTTPException(status_code=404, detail="Weather trends record not found")

@router.get("/analytics/summary", response_model=models.AnalyticsSummary)
async def get_analytics_summary(db: Session = Depends(get_db)):
    return crud.AnalyticsCRUD.get_summary(db=db)