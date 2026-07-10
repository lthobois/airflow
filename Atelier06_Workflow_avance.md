# Atelier 06 - Workflow avance

Objectif : structurer le pipeline avec Task Groups, sensor, branchement et mapping dynamique.

## 1. Créer un DAG avancé

```bash
cat > dags/ecommerce_04_advanced_workflow.py <<'PY'
from datetime import datetime
from pathlib import Path

import pandas as pd

from airflow.sdk import DAG, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import BranchPythonOperator


INPUT_FILE = "/opt/airflow/data/input/orders_2026-07-01.csv"
OUTPUT_FILE = "/opt/airflow/data/output/paid_orders_2026-07-01.csv"


def choose_branch():
    df = pd.read_csv(INPUT_FILE)
    paid_count = len(df[df["status"] == "paid"])
    if paid_count > 0:
        return "transform_orders.filter_paid_orders"
    return "skip_processing"


with DAG(
    dag_id="ecommerce_04_advanced_workflow",
    description="Task groups, sensor, branch and dynamic mapping",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "advanced"],
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_orders_file = FileSensor(
        task_id="wait_for_orders_file",
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    branch_on_paid_orders = BranchPythonOperator(
        task_id="branch_on_paid_orders",
        python_callable=choose_branch,
    )

    skip_processing = EmptyOperator(task_id="skip_processing")

    @task_group(group_id="transform_orders")
    def transform_orders():
        @task
        def filter_paid_orders():
            df = pd.read_csv(INPUT_FILE)
            paid = df[df["status"] == "paid"].copy()
            Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
            paid.to_csv(OUTPUT_FILE, index=False)
            return OUTPUT_FILE

        @task
        def list_countries(file_path: str):
            df = pd.read_csv(file_path)
            return sorted(df["country"].unique().tolist())

        @task
        def print_country(country: str):
            print(f"Orders detected for country: {country}")

        paid_file = filter_paid_orders()
        countries = list_countries(paid_file)
        print_country.expand(country=countries)

    transformed = transform_orders()

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> wait_for_orders_file >> branch_on_paid_orders
    branch_on_paid_orders >> transformed >> end
    branch_on_paid_orders >> skip_processing >> end
PY
```

DAG : ce DAG introduit des fonctionnalités avancées : sensor, branchement conditionnel, Task Group et dynamic task mapping. Il faut mettre en avant la lisibilité du graphe et la capacité à créer des tâches dynamiquement à partir des données.
- `start` : marque le début du workflow.
- `wait_for_orders_file` : attend la présence du fichier CSV avec un `FileSensor`.
- `branch_on_paid_orders` : choisit la suite du traitement avec un `BranchPythonOperator`.
- `skip_processing` : représente le chemin ignoré lorsqu'aucune commande payée n'est disponible.
- `transform_orders.filter_paid_orders` : filtre les commandes payées et écrit un fichier de sortie.
- `transform_orders.list_countries` : lit le fichier filtré et retourne la liste des pays présents.
- `transform_orders.print_country` : crée une tâche mappée par pays avec le dynamic task mapping.
- `end` : termine le workflow en acceptant qu'une des branches ait été ignorée.

Puis patientez en vérifiant que le DAG est connu d'Airflow :

```bash
docker compose exec airflow-scheduler airflow dags list | grep ecommerce_04
```

## 2. Ajouter la connexion filesystem

Le `FileSensor` utilise le système de fichiers local du conteneur.

```bash
docker compose exec airflow-scheduler airflow connections add fs_default \
  --conn-type fs \
  --conn-extra '{"path": "/"}'
```

Si la connexion existe déjà, l'erreur peut être ignorée pour cet atelier.

Cette commande crée une connexion Airflow nommée fs_default de type filesystem.
Elle sert au FileSensor qui vérifie l’existence d’un fichier dans le système de fichiers du conteneur, ici par exemple :
/opt/airflow/data/input/orders_2026-07-01.csv
Le paramètre {"path": "/"} dit simplement : le point de départ du filesystem est la racine / du conteneur.

## 3. Déclencher le DAG

Dans l'interface, déclenchez :

```text
ecommerce_04_advanced_workflow
```

Observez :

- le sensor `wait_for_orders_file` ;
- le branchement conditionnel ;
- le groupe `transform_orders` ;
- les tâches mappées par pays.

La tâche `skip_processing` doit apparaître comme ignorée/skipped si le fichier contient au moins une commande payée. Ce n'est pas une erreur : le branchement a choisi le chemin `transform_orders`.

Le message suivant est donc normal :

```text
Task was skipped - no logs available.
```

## 4. Vérifier le fichier produit

```bash
cat data/output/paid_orders_2026-07-01.csv
```

Le fichier ne doit contenir que les commandes avec `status=paid`.

## 5. Comprendre le mapping dynamique

La tâche `print_country` est créée dynamiquement pour chaque pays trouvé dans le fichier.

Dans l'interface, ouvrez le groupe `transform_orders` et observez les instances mappées.

## 6. Point de contrôle

Vous devez savoir :

- utiliser un `FileSensor` ;
- lire un Task Group dans la Graph View ;
- expliquer un branchement conditionnel ;
- expliquer pourquoi le dynamic mapping évite de coder une tâche par pays.
