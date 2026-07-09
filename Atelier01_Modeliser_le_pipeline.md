# Atelier 01 - Modeliser le pipeline e-commerce

Objectif : transformer un besoin métier simple en workflow Airflow.

## 1. Préparer l'environnement Windows et Linux

Cette section est à réaliser uniquement si l'environnement de formation n'est pas déjà prêt.

Ouvrez un terminal Windows en mode administrateur, puis activez WSL2 et les composants de virtualisation :

```cmd
dism /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
dism /online /enable-feature /featurename:HypervisorPlatform /all /norestart
```

Redémarrez ensuite la machine.

Après le redémarrage, ouvrez le Microsoft Store et installez :

```text
Debian
```

Installez Visual Studio Code depuis un terminal Windows :

```cmd
winget install Microsoft.VisualStudioCode
```

Ouvrez Debian, puis clonez le dépôt des ateliers :

```bash
mkdir -p ~/PLB/Airflow
cd ~/PLB/Airflow
git clone https://github.com/lthobois/airflow.git Ateliers
cd Ateliers
```

Le dossier courant doit maintenant contenir les fichiers des ateliers.

## 2. Se placer dans le dossier des ateliers

Si vous avez cloné le dépôt dans Debian avec la procédure précédente, utilisez plutôt :

```bash
cd ~/PLB/Airflow/Ateliers
pwd
```

Vous devez être dans le dossier qui contient `docker-compose.yml`.

## 3. Observer les données d'entrée

```bash
ls -R data
cat data/input/orders_2026-07-01.csv
```

Le fichier représente des commandes e-commerce reçues pour une journée.

## 4. Identifier les étapes du workflow

Le pipeline cible contiendra progressivement les étapes suivantes :

1. vérifier qu'un fichier de commandes est disponible ;
2. lire le fichier CSV ;
3. filtrer les commandes payées ;
4. calculer un résumé du lot ;
5. charger les commandes en base PostgreSQL ;
6. notifier une API locale ;
7. surveiller l'exécution dans Airflow.

## 5. Nommer les tâches Airflow

Utilisez des noms explicites en anglais :

```text
wait_for_orders_file
extract_orders
filter_paid_orders
compute_order_summary
load_orders_to_postgres
notify_api
end
```

Un bon nom de tâche décrit une action courte.

## 6. Dessiner le DAG cible

Représentation textuelle :

```text
wait_for_orders_file
        |
extract_orders
        |
filter_paid_orders
        |
compute_order_summary
        |
load_orders_to_postgres
        |
notify_api
        |
end
```

Dans les ateliers suivants, ce workflow sera enrichi avec du parallélisme, des XComs, des Task Groups et des intégrations.

## 7. Retenir le vocabulaire

- DAG : définition du workflow.
- DAG Run : une exécution datée du DAG.
- Task : une étape du workflow.
- Task Instance : une exécution concrète d'une tâche.
- Operator : le type d'action exécutée par une tâche.
- XCom : mécanisme de transmission de petites données entre tâches.

## 8. Point de contrôle

A ce stade, aucun service Docker n'est nécessaire.

Vous devez pouvoir expliquer :

- pourquoi le pipeline a un début et une fin ;
- pourquoi chaque étape peut être représentée par une tâche ;
- pourquoi Airflow est préférable à un script lancé manuellement pour ce cas.
