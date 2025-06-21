# US Accidents Bronze + Silver Feeder

## Objectif

Simulation d'un feeder incremental pour le dataset des accidents US avec une logique batch quotidienne.
Implementation des couches Bronze et Silver du lakehouse avec PySpark.

## Architecture

```
Source: sample.csv (999 records, 32 colonnes)
    ↓
Feeder App (PySpark)
    ↓
Bronze Layer (Parquet partitionne)
    ├── 2025/01/01/ → Part 1 (33%)
    ├── 2025/01/02/ → Part 1+2 (66%) 
    └── 2025/01/03/ → Part 1+2+3 (100%)
    ↓
Preprocessor App (PySpark)
    ↓
Silver Layer (3 datasets nettoyes)
    ├── accidents_clean/
    ├── weather_clean/
    └── location_clean/
```

## Structure des fichiers

```
├── src/
│   ├── feeder/
│   │   ├── feeder_app.py    # Application Bronze
│   │   └── __init__.py
│   ├── preprocessor/
│   │   ├── preprocessor_app.py  # Application Silver
│   │   └── __init__.py
│   └── common/
│       └── __init__.py
├── scripts/
│   ├── download_data.sh     # Verification sample
│   ├── check_output.sh      # Verification Bronze
│   └── validate_pipeline.sh # Validation complete
├── data/                    # sample.csv à placer ici
├── output/                  # Resultats (auto-cree)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── run-pipeline.sh          # Script principal
├── run-feeder.sh           # Bronze uniquement
└── run-preprocessor.sh     # Silver uniquement
```

## Utilisation

### 1. Preparation

```bash
# Placer sample.csv dans ./data/
cp sample.csv ./data/

# Verifier le setup
./scripts/download_data.sh

# Construire l'image Docker
docker-compose build
```

### 2. Execution

```bash
# Pipeline complet (Bronze + Silver)
docker-compose up

# Ou par etapes
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output \
  us-accidents-lakehouse ./run-feeder.sh

docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output \
  us-accidents-lakehouse ./run-preprocessor.sh
```

### 3. Verification

# Dans un nouveau terminal
docker exec -it us-accidents-lakehouse bash

```bash
# Validation complete
./scripts/validate_pipeline.sh

# Verification rapide Bronze
./scripts/check_output.sh

#Puis Exit
```

## Logique Incrementale

- **Jour 1 (2025-01-01)** : 33% des donnees (simulation nouvelles donnees)
- **Jour 2 (2025-01-02)** : 66% des donnees (historique + nouvelles)  
- **Jour 3 (2025-01-03)** : 100% des donnees (historique complet)

## Datasets Silver

1. **accidents_clean** : Dataset principal avec features derivees
2. **weather_clean** : Donnees meteo normalisees avec categories
3. **location_clean** : Donnees geo enrichies avec regions US

## Technologies

- **PySpark 3.3.2** : Traitement des donnees
- **Docker** : Containerisation 
- **Parquet + Snappy** : Format de stockage optimise
- **Print logging** : Suivi detaille sans logger

## Colonnes selectionnees (32)

ID, Source, Severity, Start_Time, End_Time, Start_Lat, Start_Lng,
City, County, State, Zipcode, Timezone, Temperature(F), Humidity(%),
Pressure(in), Visibility(mi), Wind_Direction, Wind_Speed(mph),
Precipitation(in), Weather_Condition, Bump, Crossing, Give_Way,
Junction, No_Exit, Railway, Roundabout, Station, Stop,
Traffic_Calming, Traffic_Signal, Turning_Loop