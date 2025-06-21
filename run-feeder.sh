#!/bin/bash

echo "FEEDER: Demarrage du US Accidents Bronze Feeder"

echo "FEEDER: Configuration Spark"
# Configuration Spark
export SPARK_HOME=/opt/bitnami/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

echo "FEEDER: Verification du fichier source"
# Vérifier que le fichier source existe
if [ ! -f "/app/data/sample.csv" ]; then
    echo "FEEDER: ERREUR: sample.csv non trouve dans /app/data/"
    echo "FEEDER: Veuillez placer votre fichier sample.csv dans le dossier data"
    exit 1
fi

echo "FEEDER: Dataset trouve, demarrage du traitement incremental"

echo "FEEDER: Lancement Feeder Jour 1 (2025-01-01)"
# Lancer les 3 feeders incrementaux
python3 /app/src/feeder/feeder_app.py --date 2025-01-01

echo "FEEDER: Lancement Feeder Jour 2 (2025-01-02)"
python3 /app/src/feeder/feeder_app.py --date 2025-01-02

echo "FEEDER: Lancement Feeder Jour 3 (2025-01-03)"
python3 /app/src/feeder/feeder_app.py --date 2025-01-03

echo "FEEDER: Tous les feeders termines avec succes!"
echo "FEEDER: Verifiez output/bronze/ pour les resultats"

echo "FEEDER: Resume:"
# Optionnel: afficher un résumé
find /app/output/bronze -name "*.parquet" -exec echo "FEEDER:   {}" \;

echo "FEEDER: Maintien du conteneur en vie pour inspection"
# Garder le conteneur en vie pour inspection
tail -f /dev/null