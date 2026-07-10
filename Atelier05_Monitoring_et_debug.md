# Atelier 05 - Monitoring et debug

Objectif : utiliser l'interface Airflow pour comprendre et corriger une erreur.

## 1. Créer un DAG avec une erreur volontaire

```bash
cat > dags/ecommerce_03_monitoring_debug.py <<'PY'
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="ecommerce_03_monitoring_debug",
    description="Debug a controlled failure",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "debug"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_existing_file = BashOperator(
        task_id="check_existing_file",
        bash_command="test -f /opt/airflow/data/input/orders_2026-07-01.csv",
    )

    check_missing_file = BashOperator(
        task_id="check_missing_file",
        bash_command="test -f /opt/airflow/data/input/orders_2099-01-01.csv",
    )

    end = EmptyOperator(task_id="end")

    start >> check_existing_file >> check_missing_file >> end
PY
```

DAG : ce DAG sert à apprendre le diagnostic d'une erreur contrôlée dans Airflow. Il faut mettre en avant la lecture des états, des logs, et la relance d'une tâche après correction.
- `start` : marque le début du workflow.
- `check_existing_file` : vérifie l'existence du fichier CSV réel avec `test -f`.
- `check_missing_file` : vérifie volontairement un fichier inexistant pour provoquer une erreur pédagogique.
- `end` : ne s'exécute que si les contrôles précédents réussissent.

Puis patientez en vérifiant que le DAG est connu d'Airflow :

```bash
docker compose exec airflow-scheduler airflow dags list | grep ecommerce_03
```

## 2. Déclencher le DAG

Déclenchez manuellement :

```text
ecommerce_03_monitoring_debug
```

Le DAG doit échouer sur `check_missing_file`.

## 3. Utiliser la Grid View

Dans la Grid View :

- repérez le run échoué ;
- cliquez sur la tâche en erreur ;
- ouvrez les logs.

Le log doit montrer une commande `test -f` en échec.

## 4. Utiliser la Graph View

Dans la Graph View :

- repérez la tâche verte ;
- repérez la tâche rouge ;
- vérifiez que `end` n'a pas été exécutée.

## 5. Corriger le DAG

Remplacez le nom du fichier manquant :

```bash
python3 - <<'PY'
from pathlib import Path

path = Path("dags/ecommerce_03_monitoring_debug.py")
text = path.read_text()
text = text.replace("orders_2099-01-01.csv", "orders_2026-07-01.csv")
path.write_text(text)
PY
```

Attendez que le DAG processor recharge le fichier.

## 6. Relancer uniquement la partie échouée

Dans l'interface :

1. ouvrez le run échoué ;
2. sélectionnez `check_missing_file` ;
3. utilisez `Clear` ;
4. confirmez la relance.

Le run doit terminer en succès.

## 7. Observer les durées

Dans la page du DAG, observez :

- les dates de début et de fin ;
- la durée des tâches ;
- l'historique des runs.

## 8. Point de contrôle

Vous devez savoir :

- lire les logs d'une tâche ;
- distinguer un échec de DAG et un échec de tâche ;
- corriger une erreur simple ;
- relancer une tâche sans relancer tout le DAG.
