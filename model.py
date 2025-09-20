from dataloader import *
from schema import *
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import logging
import time

class EcommerceETL:
    def __init__(self, model_class):
        self.model_class = model_class
        self.engine = model_class.engine
        self.Session = sessionmaker(bind=self.engine)

        logging.basicConfig(level=logging.INFO, force=True)
        self.logger = logging.getLogger(__name__)

    def extract_raw_data(self, datasets):
        """Extract raw data from given sources"""
        self.raw_data = datasets
        print("yes")
        self.logger.info(f"✅ Extracted datasets: {list(datasets.keys())}")
        return datasets

    def transform_and_load_dimensions(self):
        """Transform and load dimensions into DB"""
        session = self.Session()
        try:
            self._load_dim_date(session)
            self._load_dim_customer(session)
            self._load_dim_product(session)
            session.commit()

            self._load_fact_sales(session)


            session.commit()
            self.logger.info("✅ Dimensions loaded successfully!")
        except Exception as e:
            session.rollback()
            self.logger.error(f"❌ ETL Error: {e}")
        finally:
            session.close()


    def _load_fact_sales_old(self, session):
        self.logger.info("💰 Loading fact_sales...")
        fact_sales = []
         # Prepare mappings from raw IDs to surrogate keys
        customer_map = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", self.model_class.engine)
        product_map = pd.read_sql("SELECT product_key, stock_code FROM dim_product", self.model_class.engine)

        #dicts for faster look up

        customer_dict = dict(zip(customer_map["customer_id"], customer_map["customer_key"]))
        product_dict = dict(zip(product_map["stock_code"], product_map["product_key"]))


        '''
        TODO: Makes it faster 
transactions_df['InvoiceDate'] = pd.to_datetime(transactions_df['InvoiceDate'])
transactions_df['date_key'] = transactions_df['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)

# Map surrogate keys using vectorized pandas map
transactions_df['customer_key'] = transactions_df['CustomerID'].map(customer_dict)
transactions_df['product_key'] = transactions_df['StockCode'].map(product_dict)

# Calculate revenue
transactions_df['revenue'] = transactions_df['Quantity'] * transactions_df['UnitPrice']

        '''
        for i, row in self.raw_data["transactions"].iterrows():
          fact_sales.append({
              "sales_key": i,
              "invoice_no": row["InvoiceNo"],
              "date_key": int(pd.to_datetime(row["InvoiceDate"]).strftime('%Y%m%d')),
              "customer_key": customer_dict.get(row["CustomerID"]),
              "product_key": product_dict.get(row["StockCode"]),
              "quantity": row["Quantity"],
              "unit_price": row["UnitPrice"],
              "revenue": row["Quantity"]*row["UnitPrice"]
          })
        
        session.bulk_insert_mappings(self.model_class.FactSales, fact_sales)
        self.logger.info(f"✅ {len(fact_sales)} fact records loaded")
    def _load_fact_sales(self, session): #   def _load_fact_sales(self, session):
        self.logger.info("💰 Loading fact_sales (vectorized)...")

        # Prepare mappings from raw IDs to surrogate keys
        customer_map = pd.read_sql(
            "SELECT customer_key, customer_id FROM dim_customer",
            self.model_class.engine
        )
        product_map = pd.read_sql(
            "SELECT product_key, stock_code FROM dim_product",
            self.model_class.engine
        )
        self.logger.info(product_map.head(3))
        
        transactions = self.raw_data["transactions"].copy()
        # 1. Normalize transactions columns
        transactions['StockCode'] = transactions['StockCode'].astype(str).str.strip().str.upper()
        transactions['CustomerID'] = transactions['CustomerID'].astype(pd.Int64Dtype())  # nullable integer

        # 2. Normalize dimension maps
        product_map['stock_code'] = product_map['stock_code'].astype(str).str.strip().str.upper()
        customer_map['customer_id'] = customer_map['customer_id'].astype(int)

        # 3. Build mapping dicts
        product_dict = dict(zip(product_map['stock_code'], product_map['product_key']))
        customer_dict = dict(zip(customer_map['customer_id'], customer_map['customer_key']))

        # 4. Map keys
        transactions['product_key'] = transactions['StockCode'].map(product_dict)
        transactions['customer_key'] = transactions['CustomerID'].map(customer_dict)

        count = 0
        print(transactions['StockCode'].head(3))
        print(product_map.head(3))
        for i in transactions['StockCode']:
            if i not in product_dict.keys():
                print("Missing STOCK CODE",i)
                count += 1
                if count > 10:
                  break

        # 5. Check missing keys
        missing_products = transactions[transactions['product_key'].isna()]['StockCode'].unique()
        missing_customers = transactions[transactions['customer_key'].isna()]['CustomerID'].unique()

        print("Missing product keys:", missing_products)
        print("Missing customer keys:", missing_customers)

        #transactions['product_key'] = transactions['StockCode'].str.strip().str.upper().map(product_dict)
        transactions['date_key'] = pd.to_datetime(transactions['InvoiceDate']).dt.strftime('%Y%m%d').astype(int)
        transactions['revenue'] = transactions['Quantity'] * transactions['UnitPrice']

        # Add a sales_key column (optional, can just use index)
        transactions['sales_key'] = transactions.index

        # Assign dummy keys for missing
        dummy_customer_key = 0
        dummy_product_key = 0

        transactions['customer_key'] = transactions['customer_key'].fillna(dummy_customer_key)
        transactions['product_key'] = transactions['product_key'].fillna(dummy_product_key)

        transactions = transactions.dropna(subset=['customer_key', 'product_key'])
        invalid_stockcodes = set(transactions['StockCode']) - set(product_dict.keys())
        print("StockCodes not in dim_product:", list(invalid_stockcodes)[:20])


        # Keep only the columns needed for the fact table
        fact_sales = transactions[['sales_key', 'InvoiceNo', 'date_key', 'customer_key',
                                   'product_key', 'Quantity', 'UnitPrice', 'revenue']].rename(
            columns={'Quantity': 'quantity', 'UnitPrice': 'unit_price', 'InvoiceNo':'invoice_no'}
        ).to_dict(orient='records')

        # Bulk insert
        session.bulk_insert_mappings(self.model_class.FactSales, fact_sales)
        self.logger.info(f"✅ {len(fact_sales)} fact records loaded (vectorized)")

    def _load_dim_date(self, session):
        self.logger.info("📅 Loading date dimension...")
        start_date = datetime(2010, 1, 1)
        end_date = datetime(2012, 12, 31)

        records = []
        current_date = start_date
        while current_date <= end_date:
            records.append({
                "date_key": int(current_date.strftime('%Y%m%d')),
                "full_date": current_date.date(),
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day,
                "week": current_date.isocalendar()[1],
                "weekday": current_date.strftime('%A'),
                "is_weekend": current_date.weekday() >= 5
            })
            current_date += timedelta(days=1)

        session.bulk_insert_mappings(self.model_class.DimDate, records)
        self.logger.info(f"✅ {len(records)} date records loaded")

    def _load_dim_customer(self, session):
        self.logger.info("👥 Loading customer dimension...")
        customers = self.raw_data["customers"]
        records = []
        for i, row in customers.iterrows():
            records.append({
                "customer_id": row["CustomerID"],
                "country": row.get("Country", "Unknown")
            })
        session.bulk_insert_mappings(self.model_class.DimCustomer, records)
        self.logger.info(f"✅ {len(records)} customer records loaded")
    def _load_dim_product(self, session):
        self.logger.info("📦 Loading product dimension...")

        # Original products table
        products = self.raw_data["products"].copy()
        products['StockCode'] = products['StockCode'].astype(str).str.strip().str.upper()
        products['Description'] = products['Description'].fillna("No description")

        # Drop exact duplicates (StockCode + Description)
        products = products.drop_duplicates(subset=['StockCode'])

        # Collect all StockCodes from transactions
        transactions = self.raw_data.get("transactions")
        if transactions is not None:
            transactions_codes = transactions['StockCode'].astype(str).str.strip().str.upper().unique()
            missing_codes = set(transactions_codes) - set(products['StockCode'])
            self.logger.info("Checking missing cause transaction is not null")
            self.logger.info(f"Total transaction StockCodes: {len(transactions_codes)}")
            self.logger.info(f"Total product StockCodes: {len(products['StockCode'])}")
            self.logger.info(f"Missing codes count: {len(missing_codes)}")

            if missing_codes:
                self.logger.info(f"Adding {len(missing_codes)} missing StockCodes to dim_product...")
                missing_df = pd.DataFrame({
                    'StockCode': list(missing_codes),
                    'Description': 'Unknown Product'
                })
                products = pd.concat([products, missing_df], ignore_index=True)

        # Prepare records for bulk insert
        records = []
        for i, row in products.iterrows():
            records.append({
                "stock_code": row["StockCode"],
                "description": row.get("Description", "No description")
            })

        # Bulk insert into dim_product
        session.bulk_insert_mappings(self.model_class.DimProduct, records)
        self.logger.info(f"✅ {len(records)} product records loaded")
        time.sleep(1)

    def run_query(self, sql):
        """
        Run a raw SQL query on the warehouse and return a Pandas DataFrame.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            # Convert results to DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

Base.metadata.drop_all(warehouse.engine)  # Drops old tables
Base.metadata.create_all(warehouse.engine)

# 🔹 Create dimensions from raw data (cleaned) 🔹

# Customers: drop duplicates and missing IDs
customers_df = main_dataset[['CustomerID', 'Country']].dropna(subset=['CustomerID']).drop_duplicates(subset=['CustomerID'])
customers_df['CustomerID'] = customers_df['CustomerID'].astype(int)

# Products: drop duplicates, drop missing StockCode, normalize StockCode, fill missing description
products_df = main_dataset[['StockCode', 'Description']].copy()

# Drop rows where StockCode is missing or empty after stripping
products_df['StockCode'] = products_df['StockCode'].astype(str).str.strip().str.upper()
products_df = products_df[products_df['StockCode'] != '']
products_df = products_df.drop_duplicates(subset=['StockCode'])

# Fill missing descriptions
products_df['Description'] = products_df['Description'].fillna("No description")

# Suppliers: dummy table if none exists
suppliers_df = pd.DataFrame({"SupplierID": [1], "Name": ["Default Supplier"]})

# Transactions: drop duplicates, drop rows with missing critical info, normalize StockCode
transactions_df = main_dataset[['InvoiceNo', 'CustomerID', 'UnitPrice', 'Quantity', 'StockCode', 'InvoiceDate']].dropna()
transactions_df = transactions_df.drop_duplicates()

transactions_df['StockCode'] = transactions_df['StockCode'].astype(str).str.strip().str.upper()



# Pass to ETL
datasets = {
    "customers": customers_df,
    "products": products_df,
    "suppliers": suppliers_df,
    "transactions": transactions_df
}

etl = EcommerceETL(warehouse)
etl.extract_raw_data(datasets)
etl.transform_and_load_dimensions()
class SQLQueries:
    def __init__(self, etl):
        self.etl = etl

    # Top N customers by revenue
    def top_n_customers(self, n=10):
        padf = self.etl.run_query(f"""
            SELECT c.customer_id, SUM(f.revenue) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.customer_id
            ORDER BY revenue DESC
            LIMIT {n}
        """)
        return padf

    # Top sales by country
    def top_sales_by_country(self):
        padf = self.etl.run_query(f"""
            SELECT c.country, SUM(f.revenue) AS revenue
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.country
            ORDER BY revenue DESC
        """)
        return padf

    # Top N products by revenue
    def top_n_products(self, n=10):
        padf =  self.etl.run_query(f"""
            SELECT p.description, SUM(f.revenue) AS revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.description
            ORDER BY revenue DESC
            LIMIT {n}
        """)
        return padf

    # Monthly sales trend
    def monthly_sales_trend(self):
        
        padf = self.etl.run_query(f"""
        SELECT 
            substr(f.date_key,1,6) AS year_month,
            SUM(f.revenue) AS revenue,
            COUNT(DISTINCT f.invoice_no) AS orders,
            COUNT(DISTINCT f.customer_key) AS customers
        FROM fact_sales f
        GROUP BY year_month
        ORDER BY year_month
    """)
        return padf


    # RFM-style customer summary
    def customer_rfm_summary(self):
        padf = self.etl.run_query(f"""
            SELECT 
                f.customer_key,
                MAX(f.date_key) AS last_purchase,
                COUNT(DISTINCT f.invoice_no) AS frequency,
                SUM(f.revenue) AS monetary
            FROM fact_sales f
            GROUP BY f.customer_key
        """)
        return padf


queries = SQLQueries(etl)
customer_map = pd.read_sql(
    "SELECT customer_key, customer_id FROM dim_customer",
    etl.model_class.engine
)
product_map = pd.read_sql(
    "SELECT product_key, stock_code FROM dim_product",
    etl.model_class.engine
)

# dicts for faster lookup
customer_dict = dict(zip(customer_map["customer_id"], customer_map["customer_key"]))
product_dict = dict(zip(product_map["stock_code"], product_map["product_key"]))

#print(customer_dict)
#print(product_dict)
'''
ppadf = etl.run_query(f"""
        SELECT *
        FROM fact_sales f
    """)
print (ppadf.head())
'''


# Top 10 customers
top_customers = queries.top_n_customers(10)
print(top_customers)

# Top sales by country
country_sales = queries.top_sales_by_country()
print(country_sales)

# Top 10 products
top_products = queries.top_n_products(10)
print(top_products)

# Monthly sales trend
monthly_trend = queries.monthly_sales_trend()
print(monthly_trend)

# Customer RFM summary
rfm_summary = queries.customer_rfm_summary()
print(rfm_summary.head())

