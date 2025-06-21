FROM bitnami/spark:3.3.2

USER root

# Installer Python et les dépendances
RUN apt-get update && apt-get install -y \
    python3-pip \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installer les packages Python pour Spark
RUN pip3 install \
    pyspark==3.3.2 \
    pandas==2.0.3 \
    numpy==1.24.3

WORKDIR /app

# Copier les fichiers du projet
COPY src/ src/
COPY data/ data/
COPY scripts/ scripts/
COPY run-pipeline.sh .
COPY run-feeder.sh .
COPY run-preprocessor.sh .
COPY requirements.txt .

# Installer les dépendances Python
RUN pip3 install -r requirements.txt

# Créer les dossiers de sortie
RUN mkdir -p output/bronze output/silver output/gold logs && \
    chmod -R 777 output logs && \
    chmod +x *.sh && \
    chmod +x scripts/*.sh

CMD ["./run-pipeline.sh"]