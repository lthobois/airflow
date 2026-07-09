# Atelier 08 - PostgreSQL et API locale

Objectif : intégrer Airflow avec PostgreSQL et une API HTTP locale.

Durée indicative : 75 minutes.

## 1. Vérifier les connexions

Les connexions sont injectées par variables d'environnement dans `docker-compose.yml`.

```bash
docker compose exec airflow-scheduler airflow connections get ecommerce_postgres
docker compose exec airflow-scheduler airflow connections get ecommerce_api
```

## 2. Créer un DAG d'intégration

```bash
cat > dags/ecommerce_06_postgres_api.py <<'PY'
from datetime import datetime

import pandas as pd

from airflow.sdk import dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


INPUT_FILE = "/opt/airflow/data/input/orders_2026-07-01.csv"


@dag(
    dag_id="ecommerce_06_postgres_api",
    description="Load paid orders to PostgreSQL and notify local API",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "postgres", "api"],
)
def ecommerce_postgres_api():
    clean_tables = SQLExecuteQueryOperator(
        task_id="clean_tables",
        conn_id="ecommerce_postgres",
        sql="""
        TRUNCATE TABLE raw_orders;
        TRUNCATE TABLE order_batches;
        """,
    )

    @task
    def load_paid_orders() -> dict:
        df = pd.read_csv(INPUT_FILE)
        paid = df[df["status"] == "paid"].copy()

        hook = PostgresHook(postgres_conn_id="ecommerce_postgres")
        rows = [
            (
                row.order_id,
                row.customer_id,
                row.order_date,
                row.country,
                float(row.amount),
                row.status,
            )
            for row in paid.itertuples(index=False)
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

        total_amount = round(float(paid["amount"].sum()), 2)
        summary = {
            "batch_id": "orders_2026-07-01",
            "order_count": len(paid),
            "total_amount": total_amount,
        }

        hook.insert_rows(
            table="order_batches",
            rows=[(summary["batch_id"], summary["order_count"], summary["total_amount"])],
            target_fields=["batch_id", "order_count", "total_amount"],
            replace=True,
            replace_index=["batch_id"],
        )

        return summary

    summary = load_paid_orders()

    notify_api = HttpOperator(
        task_id="notify_api",
        http_conn_id="ecommerce_api",
        endpoint="/notifications/orders",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{{ ti.xcom_pull(task_ids='load_paid_orders') | tojson }}",
        log_response=True,
        pool="ecommerce_api_pool",
    )

    clean_tables >> summary >> notify_api


ecommerce_postgres_api()
PY
```

## 3. Déclencher le DAG

Déclenchez :

```text
ecommerce_06_postgres_api
```

Observez les logs de `notify_api`. La réponse doit indiquer que le batch a été accepté.

## 4. Vérifier PostgreSQL

```bash
docker compose exec business-postgres psql -U ecommerce -d ecommerce -c "SELECT * FROM raw_orders ORDER BY order_id;"
docker compose exec business-postgres psql -U ecommerce -d ecommerce -c "SELECT * FROM order_batches;"
```

Les commandes payées doivent être chargées.

## 5. Tester l'API directement

```bash
curl -X POST http://localhost:8000/notifications/orders \
  -H "Content-Type: application/json" \
  -d '{"batch_id":"manual_test","order_count":1,"total_amount":10.5}'
```

## 6. Point de contrôle

Vous devez savoir :

- utiliser une connexion Airflow ;
- charger des lignes dans PostgreSQL ;
- appeler une API HTTP depuis un DAG ;
- transmettre un résumé de traitement via XCom.
