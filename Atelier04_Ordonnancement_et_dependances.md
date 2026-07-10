# Atelier 04 - Ordonnancement et dependances

Objectif : enrichir le DAG avec planification, dépendances, retries et parallélisme.

## 1. Créer un DAG planifié

```bash
cat > dags/ecommerce_02_schedule_dependencies.py <<'PY'
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="ecommerce_02_schedule_dependencies",
    description="Scheduling and dependencies for ecommerce orders",
    start_date=datetime(2026, 7, 1),
    schedule="0 7 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["ecommerce", "schedule"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_input_file = BashOperator(
        task_id="check_input_file",
        bash_command="test -f /opt/airflow/data/input/orders_2026-07-01.csv",
    )

    count_lines = BashOperator(
        task_id="count_lines",
        bash_command="wc -l /opt/airflow/data/input/orders_2026-07-01.csv",
    )

    show_header = BashOperator(
        task_id="show_header",
        bash_command="head -n 1 /opt/airflow/data/input/orders_2026-07-01.csv",
    )

    validate_file = BashOperator(
        task_id="validate_file",
        bash_command="echo 'File validated'",
    )

    end = EmptyOperator(task_id="end")

    start >> check_input_file
    check_input_file >> [count_lines, show_header]
    [count_lines, show_header] >> validate_file >> end
PY
```

DAG : ce DAG montre comment Airflow orchestre des dépendances simples, du parallélisme et une planification quotidienne. Il faut mettre en avant `schedule`, `catchup=False`, les retries et la synchronisation de deux branches parallèles.
- `start` : marque le début du workflow avec un `EmptyOperator`.
- `check_input_file` : vérifie que le fichier CSV attendu existe avec une commande shell `test -f`.
- `count_lines` : compte les lignes du fichier avec `wc -l`.
- `show_header` : affiche l'en-tête du CSV avec `head -n 1`.
- `validate_file` : synchronise les deux contrôles précédents et confirme que le fichier est validé.
- `end` : marque la fin du workflow.

## 2. Attendre la détection du DAG

Vérifiez que le fichier est visible depuis le conteneur Airflow :

```bash
docker compose exec airflow-scheduler ls -l /opt/airflow/dags
```

Puis patientez en vérifiant que le DAG est connu d'Airflow :

```bash
docker compose exec airflow-scheduler airflow dags list | grep ecommerce_02
```

Si le DAG n'apparait pas, affichez les erreurs d'import :

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail=100 airflow-dag-processor
```

Après correction éventuelle, vous pouvez redémarrer le processeur de DAGs :

```bash
docker compose restart airflow-dag-processor airflow-scheduler
```

## 3. Déclencher manuellement le DAG

Dans l'interface Web Airflow, déclenchez :

```text
ecommerce_02_schedule_dependencies
```

Observez dans le Graph View (icone en dessous du nom du Dag) :

- une première tâche de contrôle ;
- deux tâches parallèles ;
- une tâche de synchronisation.

## 4. Observer la planification

Dans la page du DAG, repérez :

- `schedule` ;
- `catchup` ;
- le prochain run planifié ;
- les runs manuels.

Le DAG est planifié chaque jour à 07:00, mais `catchup=False` évite de rejouer automatiquement tout l'historique.

## 5. Provoquer une erreur contrôlée

Renommez temporairement le fichier d'entrée :

```bash
mv data/input/orders_2026-07-01.csv data/input/orders_2026-07-01.csv.disabled
```

Déclenchez à nouveau le DAG.

La tâche `check_input_file` doit échouer.

## 6. Restaurer le fichier

```bash
mv data/input/orders_2026-07-01.csv.disabled data/input/orders_2026-07-01.csv
```

Dans l'interface, utilisez `Clear` sur la tâche échouée pour la relancer.

## 7. Lire les retries

Ouvrez les détails de la tâche `check_input_file`.

Repérez :

- l'état ;
- le nombre d'essais ;
- les logs ;
- la date de début et de fin.

## 8. Point de contrôle

Vous devez savoir :

- configurer `schedule` ;
- expliquer `catchup=False` ;
- créer une branche parallèle ;
- relancer une tâche après correction.
