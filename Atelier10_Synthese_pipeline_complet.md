# Atelier 10 - Synthese pipeline complet

Objectif : assembler le pipeline final du fil rouge.

## 1. Créer le DAG final

```bash
cat > dags/ecommerce_08_final_pipeline.py <<'PY'
from datetime import datetime, timedelta
import logging
from pathlib import Path

import pandas as pd

from airflow.sdk import Variable, dag, task, task_group
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


logger = logging.getLogger(__name__)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="ecommerce_final_pipeline",
    description="Final ecommerce training pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="0 7 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ecommerce", "final"],
)
def ecommerce_final_pipeline():
    start = EmptyOperator(task_id="start")

    @task
    def build_config() -> dict:
        input_dir = Variable.get("input_dir")
        output_dir = Variable.get("output_dir")
        return {
            "batch_id": "orders_2026-07-01",
            "input_file": f"{input_dir}/orders_2026-07-01.csv",
            "paid_orders_file": f"{output_dir}/final_paid_orders.csv",
        }

    config = build_config()

    wait_for_orders_file = FileSensor(
        task_id="wait_for_orders_file",
        filepath="/opt/airflow/data/input/orders_2026-07-01.csv",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    @task_group(group_id="prepare_orders")
    def prepare_orders(config: dict):
        @task
        def validate_schema(config: dict) -> dict:
            df = pd.read_csv(config["input_file"])
            expected = {"order_id", "customer_id", "order_date", "country", "amount", "status"}
            missing = expected - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns: {sorted(missing)}")
            logger.info("Input schema is valid")
            return config

        @task
        def filter_paid_orders(config: dict) -> dict:
            df = pd.read_csv(config["input_file"])
            paid = df[df["status"] == "paid"].copy()
            Path(config["paid_orders_file"]).parent.mkdir(parents=True, exist_ok=True)
            paid.to_csv(config["paid_orders_file"], index=False)
            logger.info("Paid orders written to %s", config["paid_orders_file"])
            return config

        validated = validate_schema(config)
        return filter_paid_orders(validated)

    @task_group(group_id="load_orders")
    def load_orders(config: dict):
        @task
        def load_paid_orders_to_postgres(config: dict) -> dict:
            df = pd.read_csv(config["paid_orders_file"])
            hook = PostgresHook(postgres_conn_id="ecommerce_postgres")

            hook.run("TRUNCATE TABLE raw_orders;")
            hook.run("TRUNCATE TABLE order_batches;")

            rows = [
                (
                    row.order_id,
                    row.customer_id,
                    row.order_date,
                    row.country,
                    float(row.amount),
                    row.status,
                )
                for row in df.itertuples(index=False)
            ]

            hook.insert_rows(
                table="raw_orders",
                rows=rows,
                target_fields=[
                    "order_id",
                    "customer_id",
                    "order_date",
                    "country",
                    "amount",
                    "status",
                ],
                replace=True,
                replace_index=["order_id"],
            )

            total_amount = round(float(df["amount"].sum()), 2)
            summary = {
                "batch_id": config["batch_id"],
                "order_count": len(df),
                "total_amount": total_amount,
            }

            hook.insert_rows(
                table="order_batches",
                rows=[(summary["batch_id"], summary["order_count"], summary["total_amount"])],
                target_fields=["batch_id", "order_count", "total_amount"],
                replace=True,
                replace_index=["batch_id"],
            )

            logger.info("Loaded batch summary: %s", summary)
            return summary

        return load_paid_orders_to_postgres(config)

    prepared_config = prepare_orders(config)
    summary = load_orders(prepared_config)

    notify_api = HttpOperator(
        task_id="notify_api",
        http_conn_id="ecommerce_api",
        endpoint="/notifications/orders",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{{ ti.xcom_pull(task_ids='load_orders.load_paid_orders_to_postgres') | tojson }}",
        log_response=True,
        pool="ecommerce_api_pool",
    )

    end = EmptyOperator(task_id="end")

    start >> config >> wait_for_orders_file >> prepared_config >> summary >> notify_api >> end


ecommerce_final_pipeline()
PY
```

DAG : ce DAG assemble le pipeline final du fil rouge. Il faut mettre en avant l'enchaînement complet : configuration, attente du fichier, validation, transformation, chargement PostgreSQL, transmission XCom et notification API.
- `start` : marque le début du workflow final.
- `build_config` : construit les chemins et l'identifiant du batch à partir des variables Airflow.
- `wait_for_orders_file` : attend le fichier CSV avec un `FileSensor`.
- `prepare_orders.validate_schema` : vérifie que le fichier contient les colonnes requises.
- `prepare_orders.filter_paid_orders` : filtre les commandes payées et écrit le fichier `final_paid_orders.csv`.
- `load_orders.load_paid_orders_to_postgres` : vide les tables, charge les commandes payées, insère le résumé du batch et retourne ce résumé.
- `notify_api` : envoie le résumé du batch à l'API locale avec `HttpOperator`.
- `end` : marque la fin du pipeline complet.

## 2. Déclencher le DAG final

Dans Airflow, déclenchez :

```text
ecommerce_final_pipeline
```

## 3. Observer la structure

Dans la Graph View, identifiez :

- le démarrage ;
- le sensor ;
- le Task Group `prepare_orders` ;
- le Task Group `load_orders` ;
- l'appel API ;
- la fin du pipeline.

## 4. Vérifier les sorties fichier

```bash
cat data/output/final_paid_orders.csv
```

## 5. Vérifier les sorties base

```bash
docker compose exec business-postgres psql -U ecommerce -d ecommerce -c "SELECT * FROM raw_orders ORDER BY order_id;"
docker compose exec business-postgres psql -U ecommerce -d ecommerce -c "SELECT * FROM order_batches;"
```

## 6. Vérifier la notification API

Ouvrez les logs de la tâche `notify_api`.

La réponse doit contenir :

```text
accepted
```

## 7. Questions de synthèse

Répondez oralement ou par écrit :

1. Quelle tâche contrôle la présence du fichier ?
2. Quelle tâche transmet le résumé via XCom ?
3. Pourquoi les mots de passe ne sont-ils pas dans le DAG ?
4. Pourquoi utilise-t-on un pool sur l'appel API ?
5. Que faudrait-il changer pour traiter un fichier par jour ?

## 8. Nettoyage optionnel

Pour arrêter les services :

```bash
docker compose stop
```

Pour supprimer les conteneurs et volumes :

```bash
docker compose down -v
```

Attention : `docker compose down -v` supprime les bases PostgreSQL créées pour les ateliers.

## 9. Point de contrôle final

Vous avez construit un pipeline Airflow complet qui :

- attend un fichier ;
- valide son schéma ;
- transforme des données ;
- charge PostgreSQL ;
- transmet un résumé avec XCom ;
- appelle une API locale ;
- expose logs, états et historique dans l'interface Airflow.
