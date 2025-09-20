
from dataloader import *

# Database Schema Design - Dimensional Modeling for E-commerce Data Warehouse
'''
Uses sqlalchemy to create_engine for dimensional models.
Base class gotten from ext.declarative is the base for model definitions.
'''
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, Float, DateTime, Date, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import sqlite3

Base = declarative_base()
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EcommerceDimensionalModel:
    """
    Comprehensive dimensional model for e-commerce data warehouse
    Following Kimball methodology with fact and dimension tables.

    Where dimensions are descriptive and facts are statistics/numeric verbs.
    """
    def __init__(self, db_path="ecommerce_warehouse.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")

        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
    # DIMENSION TABLES
    class DimCustomer(Base):
        __tablename__ = 'dim_customer'
        customer_key = Column(Integer, primary_key=True, autoincrement=True)
        customer_id = Column(Integer, unique=True, nullable=False)
        country = Column(String(100))

    class DimProduct(Base):
        __tablename__ = 'dim_product'
        product_key = Column(Integer, primary_key=True, autoincrement=True)
        stock_code = Column(String(50), unique=True, nullable=False)
        description = Column(String(255))

    class DimDate(Base):
        __tablename__ = 'dim_date'
        date_key = Column(Integer, primary_key=True)  # YYYYMMDD format
        full_date = Column(Date, unique=True, nullable=False)
        year = Column(Integer)
        month = Column(Integer)
        day = Column(Integer)
        week = Column(Integer)
        weekday = Column(String(20))
        is_weekend = Column(Boolean)

    # FACT TABLE
    class FactSales(Base):
        __tablename__ = 'fact_sales'
        sales_key = Column(Integer, primary_key=True, autoincrement=True)
        invoice_no = Column(String(50))
        date_key = Column(Integer, ForeignKey('dim_date.date_key'), nullable=False)
        customer_key = Column(Integer, ForeignKey('dim_customer.customer_key'))
        product_key = Column(Integer, ForeignKey('dim_product.product_key'))
        quantity = Column(Integer)
        unit_price = Column(Float)
        revenue = Column(Float)  # Quantity * UnitPrice

    def create_all_tables(self):
        Base.metadata.create_all(self.engine)
        print("✅ Tables created successfully!")

        
    def get_table_info(self):
        """Get information about all tables"""
        inspector = inspect(create_engine(f'sqlite:///{self.db_path}'))
        
        print("📋 Database Tables:")
        for table in inspector.get_table_names():
            print(f"   📊 {table}")
        
        return inspector

# Create the dimensional model
print("🏭 Initializing E-commerce Dimensional Model...")
warehouse = EcommerceDimensionalModel()
warehouse.create_all_tables()
warehouse.get_table_info()

print("""
🎯 Dimensional Model Summary:

DIMENSION TABLES:
• dim_customer - Customer information (CustomerID, Country)
• dim_product - Product catalog (StockCode, Description)
• dim_date - Date dimension (Year, Month, Day, Week, Weekday)

FACT TABLES:
• fact_sales - Transaction-level sales data (InvoiceNo, Quantity, UnitPrice, Revenue)

🔑 Key Features:
✓ Star schema design with surrogate keys
✓ Date dimension for time-series analysis
✓ Supports sales & revenue analytics
✓ Ready for customer, product & time-based queries
✓ Marketing attribution modeling
""")
