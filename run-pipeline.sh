#!/bin/bash

echo "PIPELINE: Demarrage du pipeline US Accidents Lakehouse (Sample CSV Version)"
echo "=========================================================================="

# Timestamp pour les logs
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR

echo "PIPELINE: Configuration Spark"
# Configuration Spark
export SPARK_HOME=/opt/bitnami/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Fonction pour mesurer le temps d'exécution
measure_time() {
    local start_time=$(date +%s)
    local step_name="$1"
    shift
    
    echo "PIPELINE: Demarrage: $step_name"
    "$@" 2>&1 | tee "$LOG_DIR/${step_name}_$TIMESTAMP.log"
    local exit_code=$?
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        echo "PIPELINE: Termine: $step_name (Duree: ${duration}s)"
    else
        echo "PIPELINE: ERREUR: $step_name (Duree: ${duration}s, Code sortie: $exit_code)"
        exit $exit_code
    fi
}

echo "PIPELINE: Verification du fichier source"
# Vérifier que le fichier source existe - MODIFIÉ pour sample.csv
if [ ! -f "/app/data/sample.csv" ]; then
    echo "PIPELINE: ERREUR: sample.csv non trouve dans /app/data/"
    echo "PIPELINE: Veuillez placer votre fichier sample.csv dans le dossier data"
    exit 1
fi

echo "PIPELINE: Dataset sample trouve, demarrage du pipeline"
echo "PIPELINE: Traitement de 999 enregistrements avec 32 colonnes selectionnees"

# ============================================
# PHASE 1: BRONZE LAYER (FEEDER)
# ============================================
echo ""
echo "PIPELINE: PHASE 1: BRONZE LAYER - Ingestion des donnees"
echo "PIPELINE: ================================================="

measure_time "Feeder_Day1" python3 /app/src/feeder/feeder_app.py --date 2025-01-01
measure_time "Feeder_Day2" python3 /app/src/feeder/feeder_app.py --date 2025-01-02
measure_time "Feeder_Day3" python3 /app/src/feeder/feeder_app.py --date 2025-01-03

# ============================================
# PHASE 2: SILVER LAYER (PREPROCESSOR)
# ============================================
echo ""
echo "PIPELINE: PHASE 2: SILVER LAYER - Nettoyage et standardisation des donnees"
echo "PIPELINE: ======================================================================"

measure_time "Preprocessor_Day1" python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-01
measure_time "Preprocessor_Day2" python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-02
measure_time "Preprocessor_Day3" python3 /app/src/preprocessor/preprocessor_app.py --date 2025-01-03

# ============================================
# RÉSUMÉ FINAL
# ============================================
echo ""
echo "PIPELINE: RESUME DU PIPELINE"
echo "PIPELINE: ==================="
echo "PIPELINE: Toutes les etapes terminees avec succes!"
echo ""
echo "PIPELINE: Structure de sortie:"
find /app/output -name "*.parquet" -type f | head -20

echo ""
echo "PIPELINE: Statistiques des couches:"
echo "PIPELINE:   Bronze: $(find /app/output/bronze -name "*.parquet" | wc -l) fichiers"
echo "PIPELINE:   Silver: $(find /app/output/silver -name "*.parquet" | wc -l) fichiers"

echo ""
echo "PIPELINE: Distribution des enregistrements:"
for date in 01 02 03; do
    if [ -d "/app/output/bronze/2025/01/$date" ]; then
        bronze_files=$(find /app/output/bronze/2025/01/$date -name "*.parquet" | wc -l)
        echo "PIPELINE:   2025-01-$date Bronze: $bronze_files fichiers"
    fi
done

echo ""
echo "PIPELINE: Datasets Silver crees:"
for dataset in accidents_clean weather_clean location_clean; do
    if [ -d "/app/output/silver/$dataset" ]; then
        files=$(find /app/output/silver/$dataset -name "*.parquet" | wc -l)
        echo "PIPELINE:   $dataset: $files fichiers"
    fi
done

echo ""
echo "PIPELINE: Logs sauvegardes dans: $LOG_DIR/"
echo "PIPELINE: Pret pour le developpement Gold Layer et API!"

# Afficher quelques statistiques rapides
echo ""
echo "PIPELINE: Statistiques rapides (depuis sample.csv):"
echo "PIPELINE:   Entree: 999 enregistrements, 32 colonnes"
echo "PIPELINE:   Division: ~330 enregistrements par jour (incremental)"
echo "PIPELINE:   Nettoyage: Multiples verifications de qualite appliquees"
echo "PIPELINE:   Sortie: Format Parquet avec compression Snappy"

echo ""
echo "PIPELINE: Le conteneur restera actif pour inspection"
echo "PIPELINE: Lancez './scripts/validate_pipeline.sh' pour verifier les resultats"
tail -f /dev/null