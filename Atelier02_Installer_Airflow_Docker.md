# Atelier 02 - Installer Airflow avec Docker Compose

Objectif : démarrer l'environnement Airflow du fil rouge.

## 1. Vérifier les pré-requis

```bash
docker --version
docker compose version
```

Les deux commandes doivent répondre sans erreur.

## 2. Se placer dans le dossier des ateliers

```bash
cd PLB/Airflow/Ateliers
ls
```

Vous devez voir `docker-compose.yml` et `Dockerfile.airflow`.

## 3. Configurer l'utilisateur Linux pour Docker

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
cat .env
```

Cette valeur permet aux conteneurs Airflow d'écrire dans les dossiers montés sans créer de fichiers appartenant à `root`.

## 4. Construire l'image Airflow de formation

```bash
docker compose build
```

Cette image ajoute les providers PostgreSQL et HTTP utilisés plus tard.

Si une construction précédente a échoué pendant l'installation de dépendances Python, reconstruisez sans cache :

```bash
docker compose build --no-cache
```

## 5. Initialiser Airflow

```bash
docker compose up airflow-init
```

Cette commande :

- initialise la base de métadonnées Airflow ;
- crée l'utilisateur `airflow` ;
- crée un pool `ecommerce_api_pool`.

## 6. Démarrer les services

```bash
docker compose up -d
```

Vérifiez l'état :

```bash
docker compose ps
```

Les services attendus sont notamment :

- `airflow-apiserver` ;
- `airflow-scheduler` ;
- `airflow-dag-processor` ;
- `airflow-triggerer` ;
- `postgres` ;
- `business-postgres` ;
- `ecommerce-api`.

## 7. Ouvrir l'interface Airflow

Dans un navigateur :

```text
http://localhost:8080
```

Identifiants :

```text
airflow / airflow
```

Avec Airflow 3, ces identifiants sont configurés par `config/passwords.json` et par les variables `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_*` du fichier `docker-compose.yml`.

La liste des DAGs peut être vide. C'est normal.

## 8. Tester l'API locale

```bash
curl http://localhost:8000/health
```

Résultat attendu :

```json
{"status":"ok"}
```

## 9. Afficher le fichier de configuration airflow.cfg

```bash
docker compose exec airflow-scheduler cat /opt/airflow/airflow.cfg
```

Passez en revue rapidement le contenu du fichier et à l'aide des commandes suivantes observer les principaux paramètres du fichier.

```bash
docker compose exec airflow-scheduler airflow config get-value core executor
docker compose exec airflow-scheduler airflow config get-value core dags_folder
docker compose exec airflow-scheduler airflow config get-value core load_examples
docker compose exec airflow-scheduler airflow config get-value core auth_manager
docker compose exec airflow-scheduler airflow config get-value core execution_api_server_url
docker compose exec airflow-scheduler airflow config get-value database sql_alchemy_conn
docker compose exec airflow-scheduler airflow config get-value scheduler scheduler_heartbeat_sec
docker compose exec airflow-scheduler airflow config get-value logging base_log_folder
```

## 10. Tester la base métier PostgreSQL

```bash
docker compose exec business-postgres psql -U ecommerce -d ecommerce -c "\dt"
```

Vous devez voir les tables `raw_orders` et `order_batches`.

## 11. Observer les logs Airflow

```bash
docker compose logs --tail=50 airflow-scheduler
docker compose logs --tail=50 airflow-apiserver
```

Les logs servent à diagnostiquer les problèmes d'infrastructure.

## 12. Arrêter et redémarrer l'environnement

Arrêt :

```bash
docker compose stop
```

Redémarrage :

```bash
docker compose up -d
```

## 13. Point de contrôle

Avant de passer à l'atelier suivant :

- l'interface Airflow est accessible ;
- l'API locale répond ;
- la base métier contient les tables initiales ;
- les conteneurs Airflow sont démarrés.
