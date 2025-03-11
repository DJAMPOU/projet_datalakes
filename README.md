# 🚖 Big Data Pipeline - NYC Taxi Trip Duration

## 📌 Description du projet  
Ce projet met en place une **pipeline Big Data complète** pour le traitement et l’analyse des données de la compétition Kaggle **NYC Taxi Trip Duration**.  
L'objectif est de passer les données brutes à travers plusieurs étapes de transformation avant de les stocker dans une base de données SQL structurée.  

---

## 🔄 Architecture de la Pipeline  
1. **Ingestion des données (Raw Layer - S3)**  
   - Les données brutes sont téléchargées depuis l'API Kaggle.  
   - Elles sont stockées dans un **bucket S3**.  

2. **Transformation intermédiaire (Silver Layer - MySQL)**  
   - Nettoyage des données : suppression des valeurs nulles, formatage des dates, etc.  
   - Insertion des données dans une **table MySQL**.  

3. **Transformation finale (Gold Layer - MySQL)**  
   - Sélection des colonnes importantes.  
   - Calcul des distances de trajet à partir des coordonnées GPS.  
   - Stockage final dans une **table MySQL optimisée**.  

---

## 🚀 Installation et Exécution  

### 1️⃣ Cloner le projet  
```bash
git clone https://github.com/DJAMPOU/projet_datalakes.git -b master
cd projet_datalakes
```
### 2️⃣ Lancer le projet avec le script
Le script start_airflow.sh va :
- ✅ Créer les répertoires nécessaires (logs, plugins).
- ✅ Déployer le Docker Compose qui lance tous les conteneurs nécessaires.
- ✅ Afficher l’IP, le port, le user et le mot de passe pour accéder à l’interface Airflow.

```bash
chmod +x script.sh
./script.sh
```

### 3️⃣ Accéder à l’interface Airflow
Une fois le script exécuté, notez les informations affichées en console :

URL du Web Server : http://<IP>:8080
User : airflow
Mot de passe : airflow
Connectez-vous et lancez la pipeline pour traiter les données. 🚀

## 🛠 Technologies utilisées
* Airflow : Orchestration des tâches
* Docker & Docker Compose : Déploiement des services
* Kaggle API : Ingestion des données
* MySQL : Stockage des données transformées
* Localstack : Simuler un s3 en local
* Python : Manipulation et transformation des données
  
## 📜 Organisation des fichiers
- 📂 dags/ → Contient les fichiers DAGs Airflow pour l’orchestration.
- 📂 app/ → Contient les scripts d’ingestion et de transformation.
- 📄 docker-compose.yml → Fichier Docker Compose pour déployer tous les services.
- 📄 script.sh → Script pour lancer et configurer le projet.

## ✨ Contributeurs
- 👤 DJAMPOU Pedro Le Prince - Développeur Big Data
- 📧 Contact : pierrewalter24@gmail.com
