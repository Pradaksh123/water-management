# Water Management System: Real-time Data Ingestion & Analytics

This project is a comprehensive solution for managing and analyzing water quality and flow data in real-time. It features a complete data pipeline, from simulated data ingestion to a dynamic dashboard for visualization and a chatbot for interactive analysis.

## 🚀 Key Features

- **Real-time Data Simulation**: Continuously generates and updates data to simulate a live water management environment.
- **Database Integration**: Utilizes Supabase for a robust and scalable PostgreSQL database to store and manage all data.
- **Historical Data Ingestion**: Efficiently loads a large portion of historical data from CSV files.
- **Scheduled Data Updates**: Manages the ingestion of remaining data with logic to handle duplicates and ensure data integrity.
- **Interactive Dashboard**: Provides a dynamic, real-time dashboard for visualizing key water quality and flow metrics using Plotly and Streamlit.
- **Chatbot Analytics**: Integrates a conversational AI to allow for natural language queries and insights into the data.

## 📁 Project Structure

This project follows a logical folder structure to keep the codebase organized.
analytics/ # Contains files for data analysis and the Streamlit app's chatbot
app/ # Houses the Streamlit application code
dashwork/ # Separate folder for dashboard-related files
data/ # Stores both raw and processed data, including initial CSV files
database/ # Manages the database connection and schema
ingestion/ # Core of the data pipeline, including historical, real-time, and scheduled data handling
tests/ # Contains test scripts to ensure data integrity and functionality



## 🛠️ Technologies & Requirements

The project is built using Python and requires the following libraries. You can install them by running `pip install -r requirements.txt`.

- supabase 🟢
- python-dotenv 🐍
- streamlit 🎈
- plotly 📈
- pandas 🐼
- numpy 🔢
- langchain ⛓️
- google-generativeai 🤖

## ⚙️ How It Works

### Project Workflow

+----------------+ +-------------------+ +--------------------+
| CSV Files | ----> | Historical Data | ----> | Supabase DB |
| (Raw Data) | | Ingestion (80%) | | (PostgreSQL) |
+----------------+ +-------------------+ +--------------------+
^ | ^
| | |
+-------------------+ (Remaining 20%) |
| Scheduled Data | <---- | |
| Ingestion | | |
+-------------------+ | |
^ | |
| | |
+--------------------+ | |
| Real-time Data | ----> | |
| Simulation | | |
+--------------------+ | |
| | |
| v |
+--------------------+ +--------------------+ |
| Streamlit App | <---- | Live Data Query | <--------+
| (Dashboard) | | & Analytics |
+--------------------+ +--------------------+



**Database & Schema**: The project connects to a Supabase database. The schema is defined in `database/init_db.sql`, which sets up the necessary tables for water quality and flow rate data.

**Data Ingestion**:
1. **Historical Ingestion** (`ingestion/preload_historical.py`): This script ingests 80% of the initial data from the provided CSV files into the database.
2. **Real-time Simulation** (`ingestion/simulate_realtime.py`): This script generates random values for various water quality parameters and flow rates and inserts them into the database at a high frequency, simulating a live data feed.
3. **Scheduled Updates** (`ingestion/scheduler.py`): This script handles the remaining 20% of the data. It uses an upsert logic to intelligently insert new records and update existing ones, preventing duplicate values.

**Analytics & Dashboard**:
The dashboard, located in `app/main.py`, uses Plotly to create interactive visualizations of the data. This allows users to monitor metrics in real-time. The analytics folder also contains files for the project's chatbot.

**Live Integration**:
A shell script, `ingestion/start.sh`, is used to run both the data simulator and the Streamlit app concurrently. This allows for a seamless, live experience where the dashboard automatically updates with the data from the simulator.

**Dry Run & Testing**:
The `ingestion/dryrun.py` script is included to verify that the historical data ingestion correctly processes 80% of the data, ensuring the integrity of the data pipeline.

## 🚧 Challenges and Solutions

A key challenge in this project was managing the complexity of data ingestion.

1. **Pre-processing CSVs**: Initial data preparation was complex, as it required carefully pre-processing the CSV files to extract specific parameters and locations. This was critical to correctly parse the data before ingestion.
2. **The 80/20 Data Split**: Dividing the data into an 80% historical set and a 20% scheduled set required a careful implementation of data slicing logic to ensure the correct number of rows and values per parameter and location were handled.
3. **Concurrent Operations**: Ensuring that the real-time simulation and the Streamlit app could run simultaneously was a hurdle. This was resolved by using a shell script (`start.sh`) that ran one process in the background, allowing the other to execute in the foreground without conflicts.

## 📈 Features: Planned vs. Integrated

### Integrated Features
- Database Connectivity: Securely connecting to the Supabase PostgreSQL database
- Historical Data Ingestion: Importing 80% of the CSV data
- Real-time Data Simulation: Generating and pushing live data to the database
- Scheduled Data Ingestion: Handling the remaining 20% of the data using upsert logic
- Dashboard Visualization: Displaying data in a Streamlit app with Plotly charts
- Chatbot Integration: Basic conversational AI for data queries

### Planned Features
- Enhanced UI: Developing a more comprehensive user interface with better visual elements
- Database Scalability: Implementing more advanced database management techniques to handle very high-range databases and large data volumes. This includes considering database indexing and partitioning strategies
- Dynamic Column Integration: Creating a system to automatically integrate new columns or data from sensors into both the Supabase database schema and the application logic without requiring manual code changes. This would involve using dynamic SQL or schema management tools

## 🚀 Getting Started

1. Set up your Supabase database and retrieve your URL and API key.
2. Create a `.env` file in the root directory and add your Supabase credentials.
3. Install the required libraries using `pip install -r requirements.txt`.
4. Run the real-time simulation and app by executing the `start.sh` script in your terminal:









Here is a high-level overview of the project's data flow.


