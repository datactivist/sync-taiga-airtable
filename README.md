# sync-pcrs-projects

Synchronise une base airtable depuis un CSV.

## Pré-requis

- Python 3.9 ou supérieur : [Python](https://www.python.org/downloads/)
  > ⚠️ **Warning:** Cocher l'option "Add Python to PATH" lors de l'installation de Python.

S'assurer que `python` et `pip` sont dans le `PATH`. Vous pouvez vérifier cela en exécutant dans votre terminal:

```bash
python --version
pip --version
```

Le terminal est accessible via `Ctrl + Alt + T` sur Linux ou en recherchant "cmd" dans le menu Démarrer sur Windows.

## Installation

Créer un environnement virtuel:

```bash
python -m venv venv
```

Activer l'environnement virtuel:

```bash
source venv/bin/activate # Linux
venv\Scripts\activate # Windows
```

Installer les dépendances:

```bash
pip install -r requirements.txt
```

## Configuration

Créer un fichier `.env` à la racine du projet:

```bash
cp .env.example .env
```

Modifier les variables d'environnement dans le fichier `.env`:

Voir [cette documentation](https://airtable.com/developers/web/guides/personal-access-tokens) pour plus d'informations sur la création d'un token d'accès à l'API Airtable.

Les identifiants de base et de table sont disponibles dans l'URL de la base Airtable : `https://airtable.com/BASE_ID/TABLE_NAME/XXXXX`

```py
AIRTABLE_ACCESS_TOKEN="your_airtable_access_token"
AIRTABLE_BASE_ID_USERSTORY="your_airtable_base_id"
AIRTABLE_BASE_ID_TASK="your_airtable_base_id"
AIRTABLE_TABLE_NAME_USERSTORY="your_airtable_table_name"
AIRTABLE_TABLE_NAME_TASK="your_airtable_table_name"


PIVOT_COLUMN_USERSTORY="your_joint_key" # La colonne de jointure entre les deux tables
PIVOT_COLUMN_TASK="your_joint_key"
COLUMNS_TO_CHECK_USERSTORY="your,columns,to,check" # Les colonnes à vérifier pour la synchronisation, séparées par des virgules
COLUMNS_TO_CHECK_TASK="your,columns,to,check"

CSV_EXPORT_URL_USERSTORY="your_csv_export_url" # L'URL de récupération du CSV à synchroniser
CSV_EXPORT_URL_TASK="your_csv_export_url"
```

## Utilisation

Lancer le script:

```bash
python src/sync-userstory.py
python src/sync-task.py
```

## Limitations

L'API Airtable ne permet pas la création de nouvelles entrées dans les champs de sélection. Il est donc nécessaire de créer les entrées manuellement dans Airtable avant de lancer le script si elles n'existent pas déjà.
