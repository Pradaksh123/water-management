#!/bin/bash

# start.sh
# This script runs the Python data simulator and the Streamlit app concurrently.

echo "Starting the data simulator in the foreground..."
# This command now runs in the foreground, so you can see its output.
# It will block the terminal until you stop it with Ctrl+C.
py ingestion/simulate_realtime.py

echo "Starting the Streamlit water management app..."
# This command now points to your app inside the 'app' folder.
# It will automatically open your web browser with the Streamlit app.
streamlit run app/main.py

echo "All processes started."

