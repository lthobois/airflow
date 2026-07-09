# Cadrage des ateliers Apache Airflow

Ce document sert de contexte de référence pour la création des ateliers pratiques de la formation Apache Airflow.

## Formation

- Formation : Apache Airflow - Automatisation et orchestration de flux de données.
- Source programme : https://www.plb.fr/formation/apache-airflow
- Programme de référence : contenu local dans `PLB/Airflow`, organisé en 10 modules.
- Durée cible : 2 jours stricts.
- Public cible : développeurs ou profils data avec un niveau Python intermédiaire.

## Version et environnement

- Version cible d'Apache Airflow : dernière version stable identifiée lors du cadrage, Apache Airflow 3.3.0.
- Environnement d'exécution : Linux natif.
- Infrastructure : Docker Compose uniquement.
- Kubernetes : exclu du périmètre des ateliers.
- Tests automatiques : non prévus.

## Format des livrables

- Format attendu : fichiers Markdown pas à pas.
- Style : ateliers à suivre en copier-coller.
- Langue :
  - explications en français ;
  - commandes, noms de fichiers, code et identifiants en anglais.
- Corrigés séparés : non. Les ateliers doivent guider progressivement les stagiaires sans dossier de correction distinct.

## Fil rouge

Le fil rouge retenu est un pipeline e-commerce léger.

Objectif pédagogique : construire progressivement un pipeline Airflow qui traite des commandes e-commerce à partir de fichiers CSV, enrichit ou contrôle les données, charge le résultat dans PostgreSQL et interagit avec une API locale.

Le scénario doit rester simple, robuste et adapté à une formation de 2 jours. Il doit privilégier la compréhension des concepts Airflow plutôt que la complexité métier.

## Composants techniques du fil rouge

- Airflow exécuté via Docker Compose.
- PostgreSQL pour la base de métadonnées Airflow.
- PostgreSQL métier pour les données e-commerce.
- API locale Docker pour simuler un service externe.
- Dossier local monté pour les DAGs.
- Dossier local monté pour les données CSV.
- Scripts SQL éventuels dans un dossier dédié.

## Structure cible du dossier

```text
PLB/Airflow/Ateliers/
├── CADRAGE_ATELIERS.md
├── README.md
├── docker-compose.yml
├── data/
├── dags/
├── api/
├── sql/
├── Atelier01_Modeliser_le_pipeline.md
├── Atelier02_Installer_Airflow_Docker.md
├── Atelier03_Creer_un_premier_DAG.md
├── Atelier04_Ordonnancement_et_dependances.md
├── Atelier05_Monitoring_et_debug.md
├── Atelier06_Workflow_avance.md
├── Atelier07_XCom_et_TaskFlow.md
├── Atelier08_PostgreSQL_et_API_locale.md
├── Atelier09_Bonnes_pratiques_production.md
└── Atelier10_Synthese_pipeline_complet.md
```

## Découpage pédagogique prévu

| Atelier | Module associé | Objectif principal | Durée indicative |
|---|---|---|---:|
| 1 | Module 1 - Introduction à Apache Airflow | Modéliser le pipeline e-commerce comme workflow et DAG | 30 min |
| 2 | Module 2 - Architecture d'Airflow | Installer et explorer Airflow avec Docker Compose | 60 min |
| 3 | Module 3 - Création des DAGs | Créer le premier DAG du fil rouge | 60 min |
| 4 | Module 4 - Exécution des tâches et dépendances | Ajouter scheduling, dépendances, retries et catchup | 60 min |
| 5 | Module 5 - Monitoring et interface Web | Diagnostiquer un DAG en erreur depuis l'interface | 45 min |
| 6 | Module 6 - Fonctionnalités avancées | Utiliser Task Groups, sensor, branchement et mapping dynamique | 75 min |
| 7 | Module 7 - XCom | Faire circuler des compteurs et métadonnées entre tâches | 60 min |
| 8 | Module 8 - Intégrations | Charger PostgreSQL et appeler une API locale Docker | 75 min |
| 9 | Module 9 - Production et bonnes pratiques | Ajouter pools, variables, logs et checklist de production légère | 60 min |
| 10 | Module 10 - Synthèse | Assembler le pipeline complet | 90 min |

## Principes de conception des ateliers

- Chaque atelier doit être autonome à lire, mais s'inscrire dans le fil rouge.
- Les commandes doivent être copiables telles quelles depuis un terminal Linux.
- Les chemins doivent rester relatifs au dossier `PLB/Airflow/Ateliers`.
- Les étapes doivent être courtes, vérifiables et orientées action.
- Chaque atelier doit inclure des points d'observation dans l'interface Airflow.
- Les erreurs pédagogiques doivent être contrôlées et explicites, notamment dans l'atelier monitoring/debug.
- Le niveau de complexité doit rester compatible avec 2 jours de formation.

## Contraintes importantes

- Ne pas introduire Kubernetes.
- Ne pas dépendre d'une API publique externe.
- Ne pas prévoir de prérequis cloud.
- Ne pas créer de corrigés séparés.
- Ne pas transformer les ateliers en projet industriel lourd.
- Ne pas ajouter de tests automatisés sauf demande ultérieure.

## Références de contenu local

Les ateliers doivent rester cohérents avec les modules Markdown existants :

- `PLB/Airflow/Module1.md`
- `PLB/Airflow/Module2.md`
- `PLB/Airflow/Module3.md`
- `PLB/Airflow/Module4.md`
- `PLB/Airflow/Module5.md`
- `PLB/Airflow/Module6.md`
- `PLB/Airflow/Module7.md`
- `PLB/Airflow/Module8.md`
- `PLB/Airflow/Module9.md`
- `PLB/Airflow/Module10.md`

