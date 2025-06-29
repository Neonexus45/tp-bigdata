# Weather Analytics FastAPI Service

A FastAPI CRUD service for the weather accident analytics database with modular structure.

## Features

- **Full CRUD Operations**: Create, Read, Update, Delete for all analytics tables
- **Pagination Support**: Handle large datasets efficiently
- **Filtering & Search**: Filter by weather category, date ranges
- **Data Validation**: Pydantic models for request/response validation
- **Error Handling**: Comprehensive error handling with proper HTTP status codes
- **API Documentation**: Auto-generated Swagger/OpenAPI documentation
- **Health Checks**: Database connectivity monitoring

## API Endpoints

### Health Check
- `GET /api/v1/health` - Health check with database connectivity status

### Weather Accident Frequency
- `GET /api/v1/weather-frequency/` - List all weather frequency records (with pagination and filtering)
- `GET /api/v1/weather-frequency/{id}` - Get specific weather frequency record
- `POST /api/v1/weather-frequency/` - Create new weather frequency record
- `PUT /api/v1/weather-frequency/{id}` - Update weather frequency record
- `DELETE /api/v1/weather-frequency/{id}` - Delete weather frequency record

### Weather Severity Correlation
- `GET /api/v1/weather-severity/` - List all weather severity records (with pagination and filtering)
- `GET /api/v1/weather-severity/{id}` - Get specific weather severity record
- `POST /api/v1/weather-severity/` - Create new weather severity record
- `PUT /api/v1/weather-severity/{id}` - Update weather severity record
- `DELETE /api/v1/weather-severity/{id}` - Delete weather severity record

### Weather Impact Trends
- `GET /api/v1/weather-trends/` - List all weather trends records (with pagination and filtering)
- `GET /api/v1/weather-trends/{id}` - Get specific weather trends record
- `POST /api/v1/weather-trends/` - Create new weather trends record
- `PUT /api/v1/weather-trends/{id}` - Update weather trends record
- `DELETE /api/v1/weather-trends/{id}` - Delete weather trends record

### Analytics Summary
- `GET /api/v1/analytics/summary` - Get analytics summary statistics

## Query Parameters

### Pagination
- `skip`: Number of records to skip (default: 0)
- `limit`: Maximum number of records to return (default: 100, max: 1000)

### Filtering
- `weather_category`: Filter by weather category
- `analysis_date_from`: Filter records from this date
- `analysis_date_to`: Filter records to this date

## Setup and Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Configuration**:
   - Copy `config/.env.api.template` to `.env`
   - Update database connection settings

3. **Database Setup**:
   - Ensure MySQL server is running
   - Create the `accidents_analytics_db` database
   - Tables will be created automatically on first run

4. **Run the API**:
   ```bash
   python run_api.py
   ```

   Or using uvicorn directly:
   ```bash
   uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
   ```

## API Documentation

Once the server is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Testing

Run the test script to verify API functionality:
```bash
python test_api.py
```

## Project Structure

```
src/api/
├── __init__.py          # Package initialization
├── main.py              # FastAPI application setup
├── models.py            # Pydantic models for request/response
├── database.py          # Database connection and SQLAlchemy models
├── crud.py              # CRUD operations for all tables
├── routes.py            # API route definitions
└── README.md            # This documentation
```

## Database Schema

### weather_accident_frequency
- Weather accident frequency analysis data
- Fields: weather_category, temperature_category, accident_count, frequency_percentage

### weather_severity_correlation
- Weather and accident severity correlation data
- Fields: weather_category, severity_level, accident_count, correlation_score

### weather_impact_trends
- Weather impact trends over time
- Fields: period_month, weather_category, total_accidents, avg_severity, trend_direction

## Error Handling

The API returns appropriate HTTP status codes:
- `200`: Success
- `201`: Created
- `204`: No Content (for successful deletes)
- `404`: Not Found
- `422`: Validation Error
- `500`: Internal Server Error

## CORS Support

CORS is enabled for all origins to support frontend applications.

## Logging

The API includes comprehensive logging for monitoring and debugging.