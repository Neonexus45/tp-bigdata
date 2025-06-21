#!/bin/bash

echo "CHECK: Resume de sortie Bronze Layer"
echo "CHECK: ============================="

BRONZE_PATH="./output/bronze"

if [ -d "$BRONZE_PATH" ]; then
    echo "CHECK: Structure Bronze Layer:"
    find $BRONZE_PATH -type f -name "*.parquet" | sort
    echo ""
    
    echo "CHECK: Comptage des enregistrements par date:"
    for date_path in $BRONZE_PATH/2025/01/*; do
        if [ -d "$date_path" ]; then
            date=$(basename $date_path)
            file_count=$(find $date_path -name "*.parquet" | wc -l)
            total_size=$(du -sh $date_path | cut -f1)
            echo "CHECK:   2025-01-$date: $file_count fichiers, $total_size"
        fi
    done
else
    echo "CHECK: Aucune sortie bronze trouvee. Lancez le feeder d'abord."
fi