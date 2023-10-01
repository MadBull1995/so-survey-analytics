# Stack Overflow Survey Data Pipeline Project

## Motivation

The project aims to efficiently collect, process, and analyze multilingual survey data to derive valuable insights. The collected data spans multiple sectors and demographics, making it a rich resource for understanding customer sentiments, preferences, and behavior. Given the complex nature and volume of the data, a robust data pipeline is crucial for automating tasks, ensuring data quality, and facilitating scalable analytics.

## Tech Stack

- Data Ingestion: Python (Requests, BeautifulSoup)
- Data Transformation: Python (Pandas, scikit-learn)
- Data Storage: SQL Database (SQLAlchemy, psycopg2/pyodbc)
- Data Analysis and Modeling: Python (Pandas, scikit-learn)
- Data Presentation: Plotly, Dash, Flask/Django
- Monitoring and Maintenance: Airflow, Docker, Kubernetes, Jenkins

## Setup Phases

First clone the repo:
```bash
git clone https://github.com/MadBull1995/so-survey-analytics.git
cd so-survey-analytics
```

#### Requirements
- Python 3.x
- Requests
- BeautifulSoup
- SQLAlchemy

```bash
pip install requirments.txt
```

### 1. Data Ingestion

To start the pipeline script do the following:
```bash
# Within the repo root directory
cd ./data_pipeline
python main.py
```

#### Steps
- Collect data from various survey sources.
- Import the collected data into a staging area or database.

### 2. Data Transformation

#### Steps
- Clean and preprocess the data.
- Normalize multilingual text fields.
- Perform feature engineering.

### 3. Data Storage

#### Steps
- Store the clean, transformed data in a SQL database.

### 4. Data Analysis and Modeling

#### Steps
- Perform Exploratory Data Analysis (EDA).
- Build statistical models or machine learning models if required.

### 5. Data Presentation

#### Steps
- Create dashboards or reports.
- Build API endpoints for real-time access to the data.

### 6. Monitoring and Maintenance

#### Steps
- Implement data quality checks.
- Monitor the pipeline.
- Implement robust error-handling and logging mechanisms.

## Contribution

Feel free to contribute to this project by submitting pull requests or by raising issues.

---
Created with `</>` by Amit Shmulevitch, 2023.