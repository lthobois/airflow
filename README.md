# Ateliers Apache Airflow

Ces ateliers accompagnent une formation Apache Airflow de 2 jours.

Le fil rouge est un pipeline e-commerce léger :

1. réception de fichiers CSV de commandes ;
2. contrôle et transformation des données ;
3. chargement dans PostgreSQL ;
4. appel d'une API locale ;
5. supervision et durcissement progressif du pipeline.

## Pré-requis

- Linux natif.
- Docker.
- Docker Compose.
- Un terminal Bash.
- Un éditeur de texte.

## Démarrage rapide

Depuis ce dossier :

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose build
docker compose up airflow-init
docker compose up -d
```

Interface Airflow :

- URL : http://localhost:8080
- Login : `airflow`
- Password : `airflow`
- Source des identifiants : `config/passwords.json`

API locale :

- URL : http://localhost:8000
- Healthcheck : http://localhost:8000/health

Base PostgreSQL métier :

- Host depuis Airflow : `business-postgres`
- Port : `5432`
- Database : `ecommerce`
- User : `ecommerce`
- Password : `ecommerce`

## Ordre des ateliers

1. `Atelier01_Modeliser_le_pipeline.md`
2. `Atelier02_Installer_Airflow_Docker.md`
3. `Atelier03_Creer_un_premier_DAG.md`
4. `Atelier04_Ordonnancement_et_dependances.md`
5. `Atelier05_Monitoring_et_debug.md`
6. `Atelier06_Workflow_avance.md`
7. `Atelier07_XCom_et_TaskFlow.md`
8. `Atelier08_PostgreSQL_et_API_locale.md`
9. `Atelier09_Bonnes_pratiques_production.md`
10. `Atelier10_Synthese_pipeline_complet.md`
