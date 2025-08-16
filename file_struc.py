import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base directory (current folder)
base_dir = Path(__file__).parent

# List of folders and files to create
list_of_files = [
    "data/water_flow_data.csv",
    "data/water_quality_data.csv",
    "data/processed/.gitkeep",

    "database/init_db.sql",
    "database/db_connection.py",
    "database/insert_data.py",

    "ingestion/preload_historical.py",
    "ingestion/simulate_realtime.py",
    "ingestion/scheduler.py",

    "analytics/queries.py",
    "analytics/ai_query_interface.py",

    "grafana/grafana_setup.md",
    "grafana/dashboards/.gitkeep",

    "app/main.py",
    "app/routes/.gitkeep",
    "app/services/.gitkeep",
    "app/templates/.gitkeep",
    "app/static/.gitkeep",

    "tests/test_ingestion.py",
    "tests/test_queries.py",

    "README.md",
    "requirements.txt",
    "docker-compose.yml",
    "Dockerfile",
    ".env.example"
]

# Create files and directories
for filepath in list_of_files:
    filepath = base_dir / filepath
    filedir, filename = os.path.split(filepath)

    if filedir:
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Created directory: {filedir}")

    if not filepath.exists():
        with open(filepath, "w") as f:
            pass  # create empty file
        logging.info(f"Created file: {filepath}")

logging.info("✅ Project structure created successfully!")