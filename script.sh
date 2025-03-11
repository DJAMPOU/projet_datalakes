#!/bin/bash

# Définition des répertoires locaux
LOGS_DIR="./logs"
PLUGINS_DIR="./plugins"

# Vérifier si les répertoires existent, sinon les créer
mkdir -p "$LOGS_DIR" "$PLUGINS_DIR"

# Modifier les permissions pour permettre à Docker d'écrire
chmod 777 "$LOGS_DIR" "$PLUGINS_DIR"

echo "Répertoires $LOGS_DIR et $PLUGINS_DIR créés et prêts."

# Vérifier si 'docker compose' est disponible, sinon utiliser 'docker-compose'
if docker compose version &>/dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="docker-compose"
fi

# Construire les images si nécessaire
$DOCKER_COMPOSE_CMD build

# Lancer Docker Compose
$DOCKER_COMPOSE_CMD up -d

echo "Les containers démarrent avec Docker Compose..."

# Attendre quelques secondes pour que les conteneurs soient bien initialisés
sleep 5

# Obtenir l'IP du conteneur airflow-webserver
WEBSERVER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airflow-webserver)

# Vérifier si l'IP a été récupérée
if [ -z "$WEBSERVER_IP" ]; then
    echo "Impossible de récupérer l'IP du webserver. Vérifiez que le conteneur tourne bien avec 'docker ps'."
else
    echo "L'IP du webserver est : $WEBSERVER_IP"
    echo "Accédez à Airflow ici : http://$WEBSERVER_IP:8080"
    echo "Identifiants de connexion :"
    echo "Utilisateur : airflow"
    echo "Mot de passe : airflow"
fi
