#!/bin/bash

echo "SETUP: Configuration du dataset sample US Accidents"
echo "SETUP: ============================================="
echo ""
echo "SETUP: Utilisation de sample.csv pour les tests (999 enregistrements)"
echo ""
echo "SETUP: Fichier attendu: ./data/sample.csv"
echo "SETUP: Colonnes: 32 colonnes selectionnees pour l'analyse des accidents"
echo ""

if [ -f "./data/sample.csv" ]; then
    echo "SETUP: Dataset sample trouve!"
    echo "SETUP: Taille du fichier: $(du -h ./data/sample.csv | cut -f1)"
    echo "SETUP: Nombre d'enregistrements: $(tail -n +2 ./data/sample.csv | wc -l) enregistrements"
    echo ""
    echo "SETUP: Verification des colonnes:"
    head -1 ./data/sample.csv | tr ',' '\n' | nl
else
    echo "SETUP: Dataset sample non trouve."
    echo ""
    echo "SETUP: Veuillez vous assurer que sample.csv est dans le dossier ./data/ avec ces colonnes:"
    echo "SETUP:   ID, Source, Severity, Start_Time, End_Time, Start_Lat, Start_Lng,"
    echo "SETUP:   City, County, State, Zipcode, Timezone, Temperature(F), Humidity(%),"
    echo "SETUP:   Pressure(in), Visibility(mi), Wind_Direction, Wind_Speed(mph),"
    echo "SETUP:   Precipitation(in), Weather_Condition, Bump, Crossing, Give_Way,"
    echo "SETUP:   Junction, No_Exit, Railway, Roundabout, Station, Stop,"
    echo "SETUP:   Traffic_Calming, Traffic_Signal, Turning_Loop"
fi

echo ""
echo "SETUP: Pret a lancer: docker-compose up"