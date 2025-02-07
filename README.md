# Snowflake DeepSeek Integration

This repository contains a data pipeline for processing, analyzing, and uploading data to Snowflake using PySpark and DeepSeek API. The pipeline is orchestrated using Apache Airflow, and the processed data is stored in Snowflake for further analysis.

## Project Overview

The project is designed to:
1. **Process raw data** using PySpark for cleaning and transformation.
2. **Upload processed data** to Snowflake using the `COPY INTO` command.
3. **Analyze data** using the DeepSeek API for insights.
4. **Automate the workflow** using Apache Airflow DAGs.

## Directory Structure
```bash
Snowflake_DeepSeek/
│
├── src/ # Source code for data processing and analysis
│ ├── data_processing.py # PySpark script for preprocessing data
│ ├── upload_to_snowflake.py # Script to upload data to Snowflake using COPY INTO
│ ├── analyze_with_deepseek.py # Script to analyze data using DeepSeek API
│
├── dags/ # Airflow DAGs for pipeline scheduling
│ └── data_pipeline_dag.py # Airflow DAG to automate the workflow
│
├── data/ # Directory for storing data files
│ ├── raw/ # Raw data files downloaded from external sources
│ └── processed/ # Processed data files ready for upload to Snowflake
│
├── config/ # Configuration files
│ └── .env.example # Sample environment variable file
│
├── output/ # Output directory for analysis results
│ └── deepseek_analysis_output.txt # DeepSeek API analysis output
│
├── requirements.txt # List of Python dependencies
├── README.md # Project documentation file
└── .gitignore # Specifies files and folders to be ignored by Git
```

## Setup Instructions

### Prerequisites
1. **Python 3.8+**: Ensure Python is installed on your system.
2. **Apache Airflow**: For workflow orchestration.
3. **Snowflake Account**: To store and query processed data.
4. **DeepSeek API Key**: For data analysis.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/venkatasaisabbineni/Snowflake_DeepSeek.git
   cd Snowflake_DeepSeek
   ```
2. Install dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```
3. Set up environment variables:
    - Copy .env.example to .env:
    ```bash
    cp config/.env.example .env
    ```
    - Update .env with your Snowflake credentials, DeepSeek API key, and other configurations.
4. Set up Apache Airflow:
    - Initialize the Airflow database:
    ```bash
    airflow db init
    ```
    - Start the Airflow webserver:
    ```bash
    airflow webserver --port 8080
    ```
    - Start the Airflow scheduler:
    ```bash
    airflow scheduler
    ```
## Running the Pipeline
1. Place your raw data files in the data/raw/ directory.

2. Trigger the Airflow DAG:

3. Access the Airflow UI at http://localhost:8080.

4. Enable and trigger the data_pipeline_dag.

5. Check the processed data in Snowflake and analysis results in the results/ directory.
