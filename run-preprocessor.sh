#!/bin/bash

echo "PREPROCESSOR: Demarrage du US Accidents Silver Preprocessor"

echo "PREPROCESSOR: Configuration Spark"
# Configuration Spark
export SPARK_HOME=/opt/bitnami/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

echo "PREPROCESSOR: Verification de la Bronze layer"
# Vérifier que la Bronze layer existe
if [ ! -d "/app/output/bronze" ]; then
    echo "PREPROCESSOR: ERREUR: Bronze layer non trouvee. Lancez le feeder d'abord."
    exit 1
fi

echo "PREPROCESSOR: Bronze layer trouvee, demarrage du preprocessing"

echo "PREPROCESSOR: Lancement Preprocessor Jour 1 (2025-01-01)"
# Lancer les 3 preprocessors
python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-01

echo "PREPROCESSOR: Lancement Preprocessor Jour 2 (2025-01-02)"
python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-02

echo "PREPROCESSOR: Lancement Preprocessor Jour 3 (2025-01-03)"
python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-03

echo "PREPROCESSOR: Tout le preprocessing termine avec succes!"
echo "PREPROCESSOR: Verifiez output/silver/ pour les resultats"

echo "PREPROCESSOR: Resume de la Silver Layer:"
# Afficher un résumé
find /app/output/silver -name "*.parquet" -exec echo "PREPROCESSOR:   {}" \;