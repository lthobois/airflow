# Atelier 09 - Bonnes pratiques de production

Objectif : appliquer des pratiques simples de fiabilisation sans complexifier l'infrastructure.

## 1. Créer un DAG plus robuste

```bash
cat > dags/ecommerce_07_production_practices.py <<'PY'
from datetime import datetime, timedelta
import logging

import pandas as pd

from airflow.sdk import Variable, dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="ecommerce_07_production_practices",
    description="Production-oriented practices for the ecommerce pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="0 7 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ecommerce", "production"],
)
def ecommerce_production_practices():
    @task
    def read_runtime_config() -> dict:
        input_dir = Variable.get("input_dir")
        output_dir = Variable.get("output_dir")
        config = {
            "input_file": f"{input_dir}/orders_2026-07-01.csv",
            "output_file": f"{output_dir}/production_paid_orders.csv",
            "batch_id": "orders_2026-07-01",
        }
        logger.info("Runtime config: %s", config)
        return config

    @task
    def validate_input(config: dict) -> dict:
        df = pd.read_csv(config["input_file"])
        required_columns = {"order_id", "customer_id", "order_date", "country", "amount", "status"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns: {sorted(missing_columns)}")
        logger.info("Input file contains %s rows", len(df))
        return config

    @task
    def load_batch(config: dict) -> dict:
        df = pd.read_csv(config["input_file"])
        paid = df[df["status"] == "paid"].copy()
        paid.to_csv(config["output_file"], index=False)

        hook = PostgresHook(postgres_conn_id="ecommerce_postgres")
        total_amount = round(float(paid["amount"].sum()), 2)

        hook.run("TRUNCATE TABLE order_batches;")
        hook.insert_rows(
            table="order_batches",
            rows=[(config["batch_id"], len(paid), total_amount)],
            target_fields=["batch_id", "order_count", "total_amount"],
            replace=True,
            replace_index=["batch_id"],
        )

        summary = {
            "batch_id": config["batch_id"],
            "order_count": len(paid),
            "total_amount": total_amount,
        }
        logger.info("Batch summary: %s", summary)
        return summary

    config = read_runtime_config()
    validated_config = validate_input(config)
    summary = load_batch(validated_config)

    notify_api = HttpOperator(
        task_id="notify_api_with_pool",
        http_conn_id="ecommerce_api",
        endpoint="/notifications/orders",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{{ ti.xcom_pull(task_ids='load_batch') | tojson }}",
        log_response=True,
        pool="ecommerce_api_pool",
        priority_weight=5,
    )

    summary >> notify_api


ecommerce_production_practices()
PY
```

DAG : ce DAG reprend le pipeline avec des pratiques de production légères : variables Airflow, retries, logs utiles, pool et limitation des exécutions simultanées. Il faut mettre en avant la séparation entre configuration et code.
- `read_runtime_config` : lit les variables Airflow `input_dir` et `output_dir`, puis construit la configuration du traitement.
- `validate_input` : contrôle que le fichier CSV contient les colonnes attendues avant de continuer.
- `load_batch` : filtre les commandes payées, écrit un fichier de sortie et insère le résumé du batch dans PostgreSQL.
- `notify_api_with_pool` : notifie l'API locale avec un `HttpOperator`, en utilisant un pool pour limiter les appels concurrents.

## 2. Vérifier les variables

Les variables `input_dir` et `output_dir` sont déjà injectées par Docker Compose.

```bash
docker compose exec airflow-scheduler airflow variables get input_dir
docker compose exec airflow-scheduler airflow variables get output_dir
```

## 3. Vérifier le pool

```bash
docker compose exec airflow-scheduler airflow pools list
```

Le pool `ecommerce_api_pool` limite les appels simultanés vers l'API.

## 4. Déclencher le DAG

Déclenchez :

```text
ecommerce_07_production_practices
```

Observez :

- les logs structurés ;
- les retries configurés ;
- le pool sur la tâche HTTP ;
- `max_active_runs=1`.

## 5. Exemple de gouvernance avec Asset

Dans une instance Airflow partagée par plusieurs équipes, il est préférable d'éviter qu'un DAG central déclenche directement tous les DAGs métiers par leur nom.

Une approche plus découplée consiste à publier un `Asset` quand une donnée est prête. Les DAGs départementaux se déclenchent ensuite parce qu'ils déclarent dépendre de cet asset.

Créez un DAG principal qui publie un asset lorsque la donnée client est prête :

```bash
cat > dags/governance_01_core_asset_producer.py <<'PY'
from datetime import datetime

from airflow.sdk import Asset, DAG
from airflow.providers.standard.operators.python import PythonOperator


client_ready = Asset("asset://dwh/core/client_ready")


def load_core_clients():
    print("Loading and validating core client data")
    print("Client data is ready for departments")


with DAG(
    dag_id="governance_01_core_asset_producer",
    description="Publish a governed asset when core client data is ready",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["governance", "core"],
) as dag:
    publish_client_ready = PythonOperator(
        task_id="publish_client_ready",
        python_callable=load_core_clients,
        outlets=[client_ready],
    )
PY
```

DAG : ce DAG représente le socle central de gouvernance. Il faut mettre en avant que le DAG ne connaît pas les DAGs consommateurs : il publie uniquement un asset métier quand la donnée est prête.
- `publish_client_ready` : simule l'alimentation et la validation de la donnée client, puis publie l'asset `asset://dwh/core/client_ready` grâce à `outlets`.

Créez ensuite un DAG départemental qui se déclenche sur cet asset :

```bash
cat > dags/governance_02_finance_asset_consumer.py <<'PY'
from datetime import datetime

from airflow.sdk import Asset, DAG
from airflow.providers.standard.operators.python import PythonOperator


client_ready = Asset("asset://dwh/core/client_ready")


def run_finance_processing():
    print("Finance department is processing ready client data")


with DAG(
    dag_id="finance_01_client_processing",
    description="Finance processing triggered by governed client asset",
    start_date=datetime(2026, 7, 1),
    schedule=[client_ready],
    catchup=False,
    tags=["finance", "governance"],
) as dag:
    traitement_finance = PythonOperator(
        task_id="traitement_finance",
        python_callable=run_finance_processing,
    )
PY
```

DAG : ce DAG représente un consommateur départemental. Il faut mettre en avant que l'équipe finance ne dépend pas directement du DAG principal, mais uniquement du contrat de donnée représenté par l'asset.
- `traitement_finance` : exécute le traitement finance lorsque l'asset `asset://dwh/core/client_ready` est mis à jour.

Déclenchez le DAG consommateur départemental :

```text
finance_01_client_processing
```

Attendez quelques secondes puis déclenchez le DAG producteur :

```text
governance_01_core_asset_producer
```

Puis observez que le DAG finance est déclenché par la mise à jour de l'asset :

```text
finance_01_client_processing
```

Observez ensuite l'asset dans l'interface web.

Ce modèle permet de limiter le couplage entre équipes :

- l'équipe plateforme contrôle les conventions d'assets ;
- les équipes métiers publient leurs DAGs via CI/CD ;
- les DAGs consommateurs ne modifient pas le DAG principal ;
- l'ajout d'un nouveau département ne nécessite pas de modifier le DAG producteur.

## 6. Checklist de production légère

Pour un DAG pédagogique prêt à être partagé :

- noms de tâches explicites ;
- pas de mot de passe en dur dans le DAG ;
- connexions gérées par Airflow ;
- variables pour les chemins configurables ;
- logs utiles ;
- retries raisonnables ;
- `catchup` choisi volontairement ;
- pool pour protéger une ressource externe ;
- DAG lisible dans la Graph View.

## 7. Point de contrôle

Vous devez savoir :

- utiliser une Variable Airflow ;
- expliquer un pool ;
- ajouter des retries ;
- expliquer un asset Airflow ;
- découpler un DAG producteur et un DAG consommateur ;
- identifier les réglages importants avant mise en production.
