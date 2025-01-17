# Architecture Décisionnel

## Description
Ce projet est un template que j'ai développé dans le cadre de mon cours d'atelier Architecture Décisionnel Datamart (TRDE704) à l'EPSI Paris et Arras. Il fournit une structure de projet organisée et des instructions détaillées pour réaliser les différentes tâches liées à la création d'un datamart.

## Fonctionnalités Principales
- **Stockage des données** : Utilisation de fichiers Parquet pour le stockage efficace des données.
- **Chargement des données** : Chargement des fichiers Parquet dans une base de données SQL.
- **Modélisation des données** : Création d'un modèle en flocon de neige dans la base de données SQL.
- **Automatisation** : Automatisation des tâches à l'aide d'Apache Airflow.

## Pile Technologique
- **Langage** : Python
- **Format de fichier** : Parquet
- **Base de données** : SQL
- **Orchestration** : Apache Airflow

## Installation
Pour installer ce projet, suivez les étapes ci-dessous :

1. Clonez le dépôt :
   ```bash
   git clone https://github.com/Babacarsar/ArchitectureDecisionnel.git
   cd ArchitectureDecisionnel
   ```

2. Installez les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

## Utilisation
Pour exécuter le projet, utilisez les commandes suivantes :

1. Lancez Airflow :
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

2. Chargez les données :
   - Assurez-vous que vos fichiers Parquet sont dans le bon répertoire.
   - Exécutez le DAG approprié dans l'interface Airflow.


## Licence
Ce projet est sous la licence BSD-3-Clause. Consultez le fichier [LICENSE](LICENSE) pour plus de détails.

## Contact
Pour toute question, vous pouvez me contacter à [ba66bacar@gmail.com].
