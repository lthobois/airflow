# Atelier 03 - Creer un premier DAG

Objectif : créer le premier DAG Airflow du fil rouge.

## 1. Créer un fichier de DAG

```bash
cat > dags/ecommerce_01_first_dag.py <<'PY'
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def print_context_message():
    print("Processing the ecommerce orders pipeline")


with DAG(
    dag_id="ecommerce_01_first_dag",
    description="First ecommerce training DAG",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "training"],
) as dag:
    start = EmptyOperator(task_id="start")

    list_input_files = BashOperator(
        task_id="list_input_files",
        bash_command="ls -l /opt/airflow/data/input",
    )

    explain_pipeline = PythonOperator(
        task_id="explain_pipeline",
        python_callable=print_context_message,
    )

    end = EmptyOperator(task_id="end")

    start >> list_input_files >> explain_pipeline >> end
PY
```

DAG : ce premier DAG sert à comprendre la structure minimale d'un workflow Airflow : un début, deux actions simples et une fin. Il faut mettre en avant la déclaration du DAG, les operators utilisés et la dépendance linéaire entre les tâches.
- `start` : marque le début logique du workflow avec un `EmptyOperator`.
- `list_input_files` : liste les fichiers disponibles dans `/opt/airflow/data/input` avec un `BashOperator`.
- `explain_pipeline` : exécute une fonction Python simple avec un `PythonOperator`.
- `end` : marque la fin logique du workflow avec un `EmptyOperator`.

## 2. Attendre la détection du DAG

```bash
docker compose logs --tail=30 airflow-dag-processor
```

Dans l'interface Airflow, recherchez :

```text
ecommerce_01_first_dag
```

## 3. Déclencher le DAG

Depuis l'interface :

1. ouvrir le DAG ;
2. cliquer sur le bouton de déclenchement manuel en haut a droite ;
3. confirmer l'exécution.

## 4. Observer la Graph View

Vérifiez l'ordre des tâches :

```text
start -> list_input_files -> explain_pipeline -> end
```

Les quatre tâches doivent passer en succès.

## 5. Lire les logs d'une tâche

Ouvrez les logs de `list_input_files`.

Vous devez voir le fichier :

```text
orders_2026-07-01.csv
```

## 6. Tester le DAG en ligne de commande

```bash
docker compose exec airflow-scheduler airflow dags list | grep ecommerce
```

Puis :

```bash
docker compose exec airflow-scheduler airflow tasks list ecommerce_01_first_dag
```

## 7. Modifier la description

Remplacez la description du DAG :

```bash
python3 - <<'PY'
from pathlib import Path

path = Path("dags/ecommerce_01_first_dag.py")
text = path.read_text()
text = text.replace(
    "First ecommerce training DAG",
    "First DAG used to explore Airflow basics",
)
path.write_text(text)
PY
```

Attendez quelques secondes, puis observez la description dans l'interface Web.

## 8. Point de contrôle

Vous devez savoir :

- où placer un DAG ;
- comment nommer un `dag_id` ;
- comment créer une dépendance avec `>>` ;
- comment consulter les logs d'une tâche.
