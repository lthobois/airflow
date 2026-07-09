# Atelier 07 - XCom et TaskFlow API

Objectif : transmettre de petites informations entre tâches avec XCom et TaskFlow.

Durée indicative : 60 minutes.

## 1. Créer un DAG TaskFlow

```bash
cat > dags/ecommerce_05_xcom_taskflow.py <<'PY'
from datetime import datetime
from pathlib import Path

import pandas as pd

from airflow.sdk import dag, task


INPUT_FILE = "/opt/airflow/data/input/orders_2026-07-01.csv"
OUTPUT_FILE = "/opt/airflow/data/output/paid_orders_taskflow.csv"


@dag(
    dag_id="ecommerce_05_xcom_taskflow",
    description="Use TaskFlow and XComs for ecommerce metrics",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "xcom", "taskflow"],
)
def ecommerce_xcom_taskflow():
    @task
    def extract_orders() -> list[dict]:
        df = pd.read_csv(INPUT_FILE)
        return df.to_dict(orient="records")

    @task
    def filter_paid_orders(orders: list[dict]) -> list[dict]:
        return [order for order in orders if order["status"] == "paid"]

    @task
    def compute_summary(orders: list[dict]) -> dict:
        total_amount = sum(float(order["amount"]) for order in orders)
        return {
            "batch_id": "orders_2026-07-01",
            "order_count": len(orders),
            "total_amount": round(total_amount, 2),
        }

    @task
    def write_paid_orders(orders: list[dict]) -> str:
        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(orders).to_csv(OUTPUT_FILE, index=False)
        return OUTPUT_FILE

    @task
    def print_summary(summary: dict, output_file: str):
        print(f"Output file: {output_file}")
        print(f"Batch: {summary['batch_id']}")
        print(f"Orders: {summary['order_count']}")
        print(f"Total amount: {summary['total_amount']}")

    orders = extract_orders()
    paid_orders = filter_paid_orders(orders)
    summary = compute_summary(paid_orders)
    output_file = write_paid_orders(paid_orders)
    print_summary(summary, output_file)


ecommerce_xcom_taskflow()
PY
```

## 2. Déclencher le DAG

Déclenchez :

```text
ecommerce_05_xcom_taskflow
```

Toutes les tâches doivent passer en succès.

## 3. Observer les XComs

Dans l'interface :

1. ouvrez le DAG run ;
2. cliquez sur une tâche ;
3. cherchez l'onglet ou la section XCom ;
4. observez la valeur retournée par `compute_summary`.

Les XComs sont utiles pour de petites données de coordination.

## 4. Vérifier le fichier produit

```bash
cat data/output/paid_orders_taskflow.csv
```

## 5. Identifier la limite des XComs

Dans cet atelier, les commandes sont peu nombreuses. En production, il ne faut pas transmettre de gros fichiers ou de gros DataFrames via XCom.

Bonne pratique :

```text
Transmettre un chemin de fichier, un identifiant de batch, un compteur ou un statut.
Ne pas transmettre un gros jeu de données.
```

## 6. Point de contrôle

Vous devez savoir :

- créer un DAG avec `@dag` ;
- créer des tâches avec `@task` ;
- expliquer le retour automatique en XCom ;
- éviter les gros payloads dans XCom.
