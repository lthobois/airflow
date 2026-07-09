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

1. [Atelier 01 - Modeliser le pipeline](Atelier01_Modeliser_le_pipeline.md)
2. [Atelier 02 - Installer Airflow avec Docker](Atelier02_Installer_Airflow_Docker.md)
3. [Atelier 03 - Creer un premier DAG](Atelier03_Creer_un_premier_DAG.md)
4. [Atelier 04 - Ordonnancement et dependances](Atelier04_Ordonnancement_et_dependances.md)
5. [Atelier 05 - Monitoring et debug](Atelier05_Monitoring_et_debug.md)
6. [Atelier 06 - Workflow avance](Atelier06_Workflow_avance.md)
7. [Atelier 07 - XCom et TaskFlow](Atelier07_XCom_et_TaskFlow.md)
8. [Atelier 08 - PostgreSQL et API locale](Atelier08_PostgreSQL_et_API_locale.md)
9. [Atelier 09 - Bonnes pratiques de production](Atelier09_Bonnes_pratiques_production.md)
10. [Atelier 10 - Synthese pipeline complet](Atelier10_Synthese_pipeline_complet.md)
