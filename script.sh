#!/bin/bash

# Définition des répertoires locaux
LOGS_DIR="./logs"
PLUGINS_DIR="./plugins"

# Vérifier si les répertoires existent, sinon les créer
mkdir -p "$LOGS_DIR" "$PLUGINS_DIR"

# Modifier les permissions pour permettre à Docker d'écrire
chmod 777 "$LOGS_DIR" "$PLUGINS_DIR"

echo "✅ Répertoires $LOGS_DIR et $PLUGINS_DIR prêts."

docker compose build
# Lancer docker compose
docker compose up -d

echo "les containers démarrent avec Docker Compose..."

# Attendre quelques secondes pour que les conteneurs démarrent
sleep 5

# Obtenir l'IP du conteneur airflow-webserver
WEBSERVER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airflow-webserver)
echo "L'IP du webserver est : $WEBSERVER_IP"
echo "vous pouvez y acceder en tapant : $WEBSERVER_IP:8080 dans votre navigateur"
echo " user : airflow"
echo "password : airflow"
