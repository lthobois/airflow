# Atelier 07 - XCom et TaskFlow API

Objectif : transmettre de petites informations entre tâches avec XCom et TaskFlow.

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

DAG : ce DAG montre l'utilisation de la TaskFlow API et des XComs implicites. Il faut mettre en avant le passage de petites données entre tâches par retour de fonction, sans manipuler directement `xcom_push` ou `xcom_pull`.
- `extract_orders` : lit le CSV et retourne les commandes sous forme de liste de dictionnaires.
- `filter_paid_orders` : reçoit la liste précédente et conserve uniquement les commandes payées.
- `compute_summary` : calcule un résumé du lot avec identifiant, nombre de commandes et montant total.
- `write_paid_orders` : écrit les commandes payées dans un fichier CSV de sortie.
- `print_summary` : lit le résumé et le chemin du fichier depuis les XComs implicites et les affiche dans les logs.

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

## 4. Créer un DAG avec XComs explicites

Ce deuxième exemple utilise des operators classiques. Il montre trois cas :

- un `BashOperator` qui pousse automatiquement sa sortie standard dans XCom ;
- un `PythonOperator` qui pousse explicitement une valeur avec `xcom_push` ;
- un `PythonOperator` qui lit plusieurs valeurs avec `xcom_pull`.

```bash
cat > dags/ecommerce_05b_xcom_operators.py <<'PY'
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def push_python_summary(**context):
    summary = {
        "batch_id": "orders_2026-07-01",
        "source": "python_operator",
        "status": "ready",
    }
    context["ti"].xcom_push(key="python_summary", value=summary)


def read_xcom_values(**context):
    ti = context["ti"]

    bash_value = ti.xcom_pull(task_ids="count_paid_orders")
    python_summary = ti.xcom_pull(
        task_ids="push_python_summary",
        key="python_summary",
    )

    print(f"Value from BashOperator: {bash_value}")
    print(f"Value from PythonOperator: {python_summary}")


with DAG(
    dag_id="ecommerce_05b_xcom_operators",
    description="Explicit XCom examples with BashOperator and PythonOperator",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "xcom", "operators"],
) as dag:
    count_paid_orders = BashOperator(
        task_id="count_paid_orders",
        bash_command="awk -F, 'NR > 1 && $6 == \"paid\" { count++ } END { print count + 0 }' /opt/airflow/data/input/orders_2026-07-01.csv",
        do_xcom_push=True,
    )

    push_python_summary = PythonOperator(
        task_id="push_python_summary",
        python_callable=push_python_summary,
    )

    read_values = PythonOperator(
        task_id="read_xcom_values",
        python_callable=read_xcom_values,
    )

    [count_paid_orders, push_python_summary] >> read_values
PY
```

DAG : ce DAG complète l'exemple TaskFlow avec des XComs explicites entre operators classiques. Il faut mettre en avant la différence entre retour automatique, push explicite et lecture avec `xcom_pull`.
- `count_paid_orders` : compte les commandes payées avec un `BashOperator` et pousse automatiquement la sortie standard dans XCom.
- `push_python_summary` : construit un petit dictionnaire Python et le pousse explicitement avec `xcom_push`.
- `read_xcom_values` : récupère la sortie du `BashOperator` et le dictionnaire du `PythonOperator` avec `xcom_pull`, puis les affiche dans les logs.

Déclenchez :

```text
ecommerce_05b_xcom_operators
```

Dans les logs de `read_xcom_values`, vous devez voir :

```text
Value from BashOperator: 4
Value from PythonOperator: {'batch_id': 'orders_2026-07-01', 'source': 'python_operator', 'status': 'ready'}
```

## 5. Vérifier le fichier produit

```bash
cat data/output/paid_orders_taskflow.csv
```

## 6. Identifier la limite des XComs

Dans cet atelier, les commandes sont peu nombreuses. En production, il ne faut pas transmettre de gros fichiers ou de gros DataFrames via XCom.

Bonne pratique :

```text
Transmettre un chemin de fichier, un identifiant de batch, un compteur ou un statut.
Ne pas transmettre un gros jeu de données.
```

## 7. Point de contrôle

Vous devez savoir :

- créer un DAG avec `@dag` ;
- créer des tâches avec `@task` ;
- expliquer le retour automatique en XCom ;
- pousser explicitement une valeur avec `xcom_push` ;
- lire une valeur avec `xcom_pull` ;
- récupérer la sortie d'un `BashOperator` via XCom ;
- éviter les gros payloads dans XCom.
