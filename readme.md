# US Accidents Lakehouse Pipeline

Pipeline de données moderne pour l'analyse des accidents de la route aux États-Unis, implémenté avec Apache Spark et une architecture lakehouse en couches (Bronze, Silver, Gold).

## 🏗️ Architecture

### Architecture Core

Le projet utilise une architecture modulaire basée sur des classes core :

- **[`Config`](src/core/config.py)** : Gestion centralisée de la configuration via variables d'environnement
- **[`Logger`](src/core/logger.py)** : Système de logging avec rotation automatique des fichiers
- **[`SparkManager`](src/core/spark_manager.py)** : Gestionnaire singleton pour les sessions Spark

### Structure du Projet

```
├── src/                          # Code source
│   ├── core/                     # Classes core
│   │   ├── config.py            # Configuration centralisée
│   │   ├── logger.py            # Système de logging
│   │   └── spark_manager.py     # Gestionnaire Spark
│   ├── feeder/                  # Bronze Layer (Ingestion)
│   │   └── feeder_app.py
│   ├── preprocessor/            # Silver Layer (Nettoyage)
│   │   └── preprocessor_app.py
│   └── utils/                   # Utilitaires
│       └── helpers.py
├── docker/                      # Configuration Docker
│   ├── Dockerfile              # Image Docker
│   ├── docker-compose.yml      # Orchestration
│   ├── requirements.txt        # Dépendances Docker
│   └── .env.docker            # Variables d'environnement Docker
├── config/                     # Configuration
│   ├── .env.template          # Template de configuration
│   └── spark-defaults.conf    # Configuration Spark
├── data/                       # Données d'entrée
├── output/                     # Données de sortie
│   ├── bronze/                # Données brutes
│   ├── silver/                # Données nettoyées
│   └── gold/                  # Données agrégées (à venir)
├── logs/                       # Fichiers de log
├── scripts/                    # Scripts utilitaires
└── .env                       # Configuration développement local
```

## 🚀 Installation et Configuration

### 1. Prérequis

- Docker et Docker Compose
- Git

### 2. Configuration

1. **Cloner le projet** :
   ```bash
   git clone <repository-url>
   cd tp_final_bdfs
   ```

2. **Configurer l'environnement de développement** :
   ```bash
   cp config/.env.template .env
   # Éditer .env selon vos besoins
   ```

3. **Préparer les données** :
   ```bash
   # Placer votre fichier sample.csv dans le dossier data/
   cp your-sample.csv data/sample.csv
   ```

### 3. Variables d'Environnement

Le fichier `.env` contient la configuration principale :

```env
# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=accidents_db
MYSQL_USER=root
MYSQL_PASSWORD=password

# Spark Configuration
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
SPARK_UI_ENABLED=true
SPARK_UI_PORT=4040

# Application Configuration
LOG_LEVEL=INFO
BATCH_SIZE=1000
OUTPUT_FORMAT=parquet

# Paths
INPUT_PATH=/app/data/sample.csv
BRONZE_OUTPUT_PATH=/app/output/bronze
SILVER_OUTPUT_PATH=/app/output/silver
```

## 🐳 Déploiement Docker

### Lancement du Pipeline Complet

```bash
# Depuis la racine du projet
./run-pipeline.sh
```

### Lancement par Composant

```bash
# Bronze Layer uniquement
./run-feeder.sh

# Silver Layer uniquement (après Bronze)
./run-preprocessor.sh
```

### Commandes Docker Manuelles

```bash
# Build et lancement depuis le dossier docker/
cd docker/
docker-compose up --build

# Lancement en arrière-plan
docker-compose up -d

# Arrêt
docker-compose down
```

## 📊 Utilisation des Classes Core

### Configuration

```python
from src.core.config import Config

# Chargement automatique du fichier .env
config = Config()

# Accès aux variables
db_host = config.get('MYSQL_HOST')
spark_memory = config.get('SPARK_DRIVER_MEMORY', '2g')
```

### Logging

```python
from src.core.logger import Logger

# Initialisation du logger
logger = Logger(__name__)

# Utilisation
logger.info("Démarrage du traitement")
logger.error("Erreur lors du traitement", exc_info=True)
```

### Spark Manager

```python
from src.core.spark_manager import SparkManager

# Récupération de la session Spark (singleton)
spark = SparkManager.get_spark()

# Utilisation normale de Spark
df = spark.read.csv("path/to/file.csv", header=True)
```

## 🔍 Monitoring et Logs

### Structure des Logs

Les logs sont organisés par composant et horodatés :

```
logs/
├── Feeder_Day1_20250621_152630.log
├── Feeder_Day2_20250621_152645.log
├── Preprocessor_Day1_20250621_152700.log
└── application_20250621.log
```

### Validation du Pipeline

```bash
# Vérification complète du pipeline
./scripts/validate_pipeline.sh
```

### Accès aux Logs

```bash
# Logs en temps réel
tail -f logs/application_$(date +%Y%m%d).log

# Recherche dans les logs
grep "ERROR" logs/*.log
```

## 🏃‍♂️ Développement

### Développement Local

1. **Installation des dépendances** :
   ```bash
   pip install -r requirements.txt
   ```

2. **Configuration** :
   ```bash
   cp config/.env.template .env
   # Adapter les chemins pour le développement local
   ```

3. **Lancement direct** :
   ```bash
   python src/feeder/feeder_app.py --date 2025-01-01
   python src/preprocessor/preprocessor_app.py --date 2025-01-01
   ```

### Tests

```bash
# Test des classes core
python test_core.py

# Validation de la configuration
python -c "from src.core.config import Config; print(Config().get_all())"
```

## 📈 Pipeline de Données

### Bronze Layer (Feeder)

- **Objectif** : Ingestion des données brutes
- **Format d'entrée** : CSV
- **Format de sortie** : Parquet partitionné par date
- **Traitement** : Validation basique, partitionnement temporel

### Silver Layer (Preprocessor)

- **Objectif** : Nettoyage et standardisation
- **Format d'entrée** : Parquet (Bronze)
- **Format de sortie** : Parquet nettoyé
- **Traitement** : 
  - Nettoyage des données manquantes
  - Standardisation des formats
  - Validation de la qualité
  - Séparation en datasets thématiques

### Gold Layer (À venir)

- **Objectif** : Agrégations et métriques business
- **Format d'entrée** : Parquet (Silver)
- **Format de sortie** : Tables optimisées pour l'analyse

## 🔧 Dépendances

### Production

```
pyspark==3.5.0
numpy==1.26.0
pyarrow==15.0.0
findspark==2.0.1
python-dotenv==1.0.0
pymysql==1.1.0
sqlalchemy==2.0.25
```

### Remarques

- **Pandas supprimé** : Remplacé par Spark DataFrame pour de meilleures performances
- **Architecture modulaire** : Classes core réutilisables
- **Configuration centralisée** : Variables d'environnement
- **Logging avancé** : Rotation automatique des fichiers

## 🚨 Dépannage

### Problèmes Courants

1. **Erreur de mémoire Spark** :
   ```bash
   # Réduire la mémoire dans .env
   SPARK_DRIVER_MEMORY=2g
   SPARK_EXECUTOR_MEMORY=2g
   ```

2. **Fichier .env non trouvé** :
   ```bash
   cp config/.env.template .env
   ```

3. **Permissions Docker** :
   ```bash
   # Vérifier les permissions des dossiers
   chmod -R 755 output/ logs/
   ```

4. **Logs non générés** :
   ```bash
   # Créer le dossier logs
   mkdir -p logs
   chmod 777 logs
   ```

### Validation de l'Installation

```bash
# Test complet
./scripts/validate_pipeline.sh

# Test des classes core
python -c "
from src.core.config import Config
from src.core.logger import Logger
from src.core.spark_manager import SparkManager

print('✓ Config OK')
print('✓ Logger OK') 
print('✓ SparkManager OK')
"
```

## 📝 Changelog

### Version 2.0 (Actuelle)

- ✅ Architecture core avec classes Config, Logger, SparkManager
- ✅ Suppression de pandas
- ✅ Réorganisation Docker dans dossier dédié
- ✅ Système de logging avancé avec rotation
- ✅ Configuration centralisée par variables d'environnement
- ✅ Scripts shell mis à jour pour Docker

### Version 1.0

- ✅ Pipeline Bronze/Silver fonctionnel
- ✅ Support Docker de base
- ✅ Traitement par batch

## 🤝 Contribution

1. Fork du projet
2. Création d'une branche feature
3. Tests des modifications
4. Pull request avec description détaillée

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails.