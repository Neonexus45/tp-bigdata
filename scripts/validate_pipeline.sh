#!/bin/bash

echo "VALIDATION: Rapport de validation du pipeline"
echo "VALIDATION: =================================="

BRONZE_PATH="./output/bronze"
SILVER_PATH="./output/silver"

echo "VALIDATION: Verification Bronze Layer"
# Validation Bronze Layer
echo ""
echo "VALIDATION: BRONZE LAYER VALIDATION"
echo "VALIDATION: -------------------------"

if [ -d "$BRONZE_PATH" ]; then
    for date in 01 02 03; do
        date_path="$BRONZE_PATH/2025/01/$date"
        if [ -d "$date_path" ]; then
            file_count=$(find $date_path -name "*.parquet" | wc -l)
            size=$(du -sh $date_path | cut -f1)
            echo "VALIDATION:   2025-01-$date: $file_count fichiers, $size"
        else
            echo "VALIDATION:   2025-01-$date: Manquant"
        fi
    done
else
    echo "VALIDATION:   Bronze layer non trouvee"
fi

echo "VALIDATION: Verification Silver Layer"
# Validation Silver Layer
echo ""
echo "VALIDATION: SILVER LAYER VALIDATION"
echo "VALIDATION: -------------------------"

if [ -d "$SILVER_PATH" ]; then
    datasets=("accidents_clean" "weather_clean" "location_clean")
    
    for dataset in "${datasets[@]}"; do
        echo "VALIDATION:   Dataset: $dataset"
        for date in 01 02 03; do
            date_path="$SILVER_PATH/$dataset/2025/01/$date"
            if [ -d "$date_path" ]; then
                file_count=$(find $date_path -name "*.parquet" | wc -l)
                size=$(du -sh $date_path | cut -f1)
                echo "VALIDATION:     2025-01-$date: $file_count fichiers, $size"
            else
                echo "VALIDATION:     2025-01-$date: Manquant"
            fi
        done
        echo ""
    done
else
    echo "VALIDATION:   Silver layer non trouvee"
fi

echo "VALIDATION: Statistiques generales"
# Validation générale
echo ""
echo "VALIDATION: STATISTIQUES GENERALES"
echo "VALIDATION: --------------------"
if [ -d "./output" ]; then
    total_files=$(find ./output -name "*.parquet" | wc -l)
    total_size=$(du -sh ./output | cut -f1)
    echo "VALIDATION:   Total fichiers: $total_files"
    echo "VALIDATION:   Taille totale: $total_size"
    echo "VALIDATION:   Structure repertoire:"
    if command -v tree >/dev/null 2>&1; then
        tree ./output -I "*.parquet" | head -20
    else
        find ./output -type d | head -20
    fi
else
    echo "VALIDATION:   Aucun repertoire output trouve"
fi