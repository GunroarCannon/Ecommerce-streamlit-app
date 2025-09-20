# E-Commerce Data Warehouse & Advanced Analytics Platform

## Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Technology Stack](#technology-stack)
4. [Project Architecture](#project-architecture)
5. [Data Modeling](#data-modeling)
6. [ETL Pipeline](#etl-pipeline)
7. [Advanced Analytics](#advanced-analytics)
8. [Dashboard and Visualization](#dashboard-and-visualization)
9. [Authentication](#authentication)
10. [Setup Instructions](#setup-instructions)
11. [Usage](#usage)
12. [Future Improvements](#future-improvements)
13. [License](#license)

---

## Project Overview

This project is a full-featured *data warehouse and analytics platform* designed for a realistic multi-channel e-commerce business. It supports:

* ETL pipelines for loading raw transactional data into structured data warehouses
* Advanced SQL-based analytics
* Interactive dashboards for stakeholders
* Optional authentication for secure access

The platform demonstrates modern *data engineering and analytics practices*, while remaining lightweight enough to run locally using SQLite or cloud-hosted solutions.

---

## Key Features

* *Data Warehouse Design:* Star/Snowflake schema with dimension and fact tables
* *ETL Pipeline:* Automates extraction, transformation, and loading of raw e-commerce data
* *Advanced Analytics:* Customer cohorts, marketing attribution, inventory management, RFM and CLV segmentation
* *Dashboard:* Streamlit-based visualization with optional authentication
* *Machine Learning:* (Optional) Churn prediction and demand forecasting

---

## Technology Stack

| Component               | Tools / Libraries                      |
| ----------------------- | -------------------------------------- |
| Data Storage            | SQLite (local), SQLAlchemy ORM         |
| ETL & Data Manipulation | Python, Pandas, NumPy                  |
| Analytics               | SQL (SQLite), Pandas                   |
| Visualization           | Streamlit, Plotly, Matplotlib, Seaborn |
| Authentication          | streamlit-authenticator              |
| ML / Forecasting        | Scikit-learn, XGBoost                  |

---

## Project Architecture


raw_data/        --> CSV / JSON / API sources
etl/             --> Python ETL scripts
warehouse.db     --> SQLite database
dashboards/      --> Streamlit apps
analytics/       --> SQL / Python notebooks


* *Dimension Tables:* dim_customer, dim_product, dim_date, dim_supplier
* *Fact Tables:* fact_sales, fact_inventory, fact_returns
* ETL scripts populate dimension tables first, then fact tables

---

## Data Modeling

### Example: Star Schema
```
        dim_date
            |
dim_customer -- fact_sales -- dim_product
            |
        dim_supplier
```



### Dimension Table Example

python
# Load products dimension
products_df['StockCode'] = products_df['StockCode'].str.upper().str.strip()
products_df['Description'] = products_df['Description'].fillna('No description')
products_df.drop_duplicates(subset=['StockCode'], inplace=True)


### Fact Table Example

python
transactions_df['date_key'] = pd.to_datetime(transactions_df['InvoiceDate']).dt.strftime('%Y%m%d').astype(int)
transactions_df['customer_key'] = transactions_df['CustomerID'].map(customer_dict)
transactions_df['product_key'] = transactions_df['StockCode'].map(product_dict)
transactions_df['revenue'] = transactions_df['Quantity'] * transactions_df['UnitPrice']


---

## ETL Pipeline

1. *Extract:* Load raw CSV/JSON/API data into Pandas DataFrames
2. *Transform:*

   * Normalize columns (uppercase, trim spaces)
   * Fill missing values
   * Map surrogate keys
   * Remove duplicates
3. *Load:*

   * Bulk insert into SQLite dimension and fact tables

python
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()

session.bulk_insert_mappings(DimProduct, product_records)
session.bulk_insert_mappings(FactSales, fact_sales_records)

session.commit()
session.close()


*Data Freshness:* Scheduled runs (cron / Airflow / manual batch) can keep warehouse updated.

---

## Advanced Analytics

Example SQL queries:

### Multi-Level Cohort Analysis

sql
SELECT
    strftime('%Y-%m', i.InvoiceDate) AS cohort_month,
    COUNT(DISTINCT f.customer_key) AS new_customers
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date i ON f.date_key = i.date_key
GROUP BY cohort_month;


### Inventory Management

sql
SELECT p.stock_code, p.description, SUM(f.quantity) AS total_sold
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.stock_code
ORDER BY total_sold DESC;


### RFM Segmentation

* *Recency:* Days since last purchase
* *Frequency:* Total number of orders
* *Monetary:* Total revenue

python
rfm = transactions_df.groupby('customer_key').agg({
    'InvoiceDate': lambda x: (pd.Timestamp.now() - x.max()).days,
    'InvoiceNo': 'nunique',
    'revenue': 'sum'
}).rename(columns={'InvoiceDate':'Recency','InvoiceNo':'Frequency','revenue':'Monetary'})


---

## Dashboard and Visualization

Streamlit app for business stakeholders:

python
import streamlit as st
import plotly.express as px

st.title("E-Commerce Analytics Dashboard")

kpi1 = transactions_df['revenue'].sum()
st.metric("Total Revenue", f"${kpi1:,.2f}")

fig = px.bar(transactions_df.groupby('StockCode')['revenue'].sum().reset_index(),
             x='StockCode', y='revenue')
st.plotly_chart(fig)


* Supports filtering by product, region, date
* Can embed *background images*, CSS, and custom fonts:

python
st.markdown(
    """
    <style>
    .stApp {
        background-image: url('https://example.com/grid.png');
        background-size: cover;
        font-family: 'Courier New', monospace;
    }
    </style>
    """, unsafe_allow_html=True
)


---

## Authentication

Using streamlit-authenticator:

python
import streamlit_authenticator as stauth

credentials = {
    "usernames": {"admin": {"name": "Admin", "password": stauth.Hasher(["password123"]).generate()[0]}}
}

authenticator = stauth.Authenticate(credentials, "cookie_name", "signature_key", cookie_expiry_days=1)
name, authentication_status, username = authenticator.login("Login", "main")


* Only authenticated users can view dashboards

---

## Setup Instructions
# Project Name

This project uses **Python** and **Streamlit** to run an interactive web application.

## Requirements
- Python 3.8+
- `pip` package manager
- `requirements.txt` file with all dependencies

## Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/GunroarCannon/Ecommerce-streamlit-app.git
   cd your-repo
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment**
   - **Windows**:
     ```bash
     venv\Scripts\activate
     ```
   - **Mac/Linux**:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Run the Streamlit app**
   ```bash
   streamlit run app.py
   ```

6. **Access the app**
   Open the URL shown in your terminal (default: http://localhost:8501) in your web browser.

## Optional: Deactivate the virtual environment
When done, deactivate with:
```bash
deactivate
```

---

## Usage

* Explore KPIs on Streamlit dashboard
* Apply filters to analyze sales trends by product, customer, or time
* Export analytics results to CSV for reporting

---

## Future Improvements

* Automate *daily batch loads* via cron or Airflow
* Add *churn prediction* and *demand forecasting models*
* Integrate external APIs (payment gateways, inventory systems)
* Add REST endpoints for automated reporting
* Enhance dashboard with drill-down visuals and multi-channel analytics

---

## License

This project is licensed under the MIT License.
