# Atelier 09 - Bonnes pratiques de production

Objectif : appliquer des pratiques simples de fiabilisation sans complexifier l'infrastructure.

Durée indicative : 60 minutes.

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
        input_dir = Variable.get("input_dir", default_var="/opt/airflow/data/input")
        output_dir = Variable.get("output_dir", default_var="/opt/airflow/data/output")
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

## 5. Checklist de production légère

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

## 6. Point de contrôle

Vous devez savoir :

- utiliser une Variable Airflow ;
- expliquer un pool ;
- ajouter des retries ;
- identifier les réglages importants avant mise en production.
