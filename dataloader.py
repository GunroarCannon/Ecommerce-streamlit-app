
# Data Acquisition - Multiple E-commerce Dataset Options
import requests
import pandas as pd
import zipfile
import os
from io import StringIO

class EcommerceDataAcquisition:
    def __init__(self):
        self.datasets = {}
        
    def download_sample_ecommerce_data(self):
        """
        Download sample e-commerce datasets from various sources
        """
        print("📥 Downloading E-commerce datasets...")
        
        # Option 1: UK E-commerce data (Online Retail dataset)
        # Yes, it exists. Should not fail
        try:
            url1 = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
          
            #too fictional and short
            #    "https://raw.githubusercontent.com/paulsamuel-w-e/E-commerce-Customer-Behaviour-Dataset/refs/heads/main/E-commerce.csv"
            #no price 
            #    https://raw.githubusercontent.com/gilangnr/e-commerce_public_dataset/refs/heads/main/all_data.csv"
            #too limited
            #    "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
            print("Downloading Online Retail dataset...")
            
            if False: #excel loading
                uk_data = pd.read_excel("Online Retail.xlsx", engine='openpyxl')
            elif True: #csv loading
                uk_data = pd.read_csv("Online Retail.csv", encoding='ISO-8859-1')
            else: #download
                uk_data = pd.read_excel(url1, engine='openpyxl')
                
            self.datasets['retail'] = uk_data
            uk_data.to_csv("Online Retail.csv", index=False )
            print(f"✅ Retail data: {uk_data.shape}")
        except Exception as e:
            print(f"❌ Retail download failed: {e}")
        
        # Option 2: Create synthetic comprehensive e-commerce data
        # not needed
        # self.create_synthetic_ecommerce_data()
        
        return self.datasets
    
    def create_synthetic_ecommerce_data(self):
        """
        Create realistic synthetic e-commerce data for the warehouse
        

        Just incase. It's a cool function.
        """
        print("🏭 Generating synthetic e-commerce data...")
        
        np.random.seed(42)
        
        # Generate customers data
        num_customers = 5000
        customers = pd.DataFrame({
            'customer_id': range(1, num_customers + 1),
            'first_name': np.random.choice(['John', 'Jane', 'Mike', 'Sarah', 'David', 'Lisa', 'Chris', 'Emma'], num_customers),
            'last_name': np.random.choice(['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson', 'Moore', 'Taylor', 'Anderson'], num_customers),
            'email': [f'user{i}@email.com' for i in range(1, num_customers + 1)],
            'registration_date': pd.date_range('2020-01-01', '2024-01-01', periods=num_customers),
            'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia'], num_customers),
            'state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA'], num_customers),
            'country': 'USA',
            'customer_segment': np.random.choice(['Premium', 'Standard', 'Basic'], num_customers, p=[0.2, 0.5, 0.3])
        })
        
        # Generate products data
        num_products = 1000
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Beauty', 'Toys']
        products = pd.DataFrame({
            'product_id': range(1, num_products + 1),
            'product_name': [f'Product {i}' for i in range(1, num_products + 1)],
            'category': np.random.choice(categories, num_products),
            'supplier_id': np.random.randint(1, 101, num_products),
            'unit_price': np.round(np.random.exponential(50) + 10, 2),
            'cost_price': lambda x: x * 0.7,  # Will be calculated
            'launch_date': pd.date_range('2020-01-01', '2023-12-31', periods=num_products),
            'brand': np.random.choice(['Brand A', 'Brand B', 'Brand C', 'Brand D', 'Brand E'], num_products)
        })
        products['cost_price'] = np.round(products['unit_price'] * 0.7, 2)
        
        # Generate suppliers data
        num_suppliers = 100
        suppliers = pd.DataFrame({
            'supplier_id': range(1, num_suppliers + 1),
            'supplier_name': [f'Supplier {i}' for i in range(1, num_suppliers + 1)],
            'contact_email': [f'supplier{i}@company.com' for i in range(1, num_suppliers + 1)],
            'country': np.random.choice(['USA', 'China', 'Germany', 'Japan', 'India'], num_suppliers),
            'rating': np.round(np.random.uniform(3.0, 5.0, num_suppliers), 1)
        })
        
        # Generate sales transactions
        num_transactions = 50000
        transactions = pd.DataFrame({
            'transaction_id': range(1, num_transactions + 1),
            'customer_id': np.random.randint(1, num_customers + 1, num_transactions),
            'product_id': np.random.randint(1, num_products + 1, num_transactions),
            'order_date': pd.date_range('2022-01-01', '2024-09-01', periods=num_transactions),
            'quantity': np.random.randint(1, 6, num_transactions),
            'channel': np.random.choice(['Website', 'Mobile App', 'Store', 'Social Media'], num_transactions, p=[0.4, 0.3, 0.2, 0.1]),
            'payment_method': np.random.choice(['Credit Card', 'PayPal', 'Debit Card', 'Bank Transfer'], num_transactions),
            'shipping_cost': np.round(np.random.uniform(0, 15, num_transactions), 2),
            'discount_applied': np.random.choice([0, 5, 10, 15, 20], num_transactions, p=[0.5, 0.2, 0.15, 0.1, 0.05])
        })
        
        # Calculate revenue
        product_prices = dict(zip(products['product_id'], products['unit_price']))
        transactions['unit_price'] = transactions['product_id'].map(product_prices)
        transactions['gross_revenue'] = transactions['quantity'] * transactions['unit_price']
        transactions['discount_amount'] = transactions['gross_revenue'] * transactions['discount_applied'] / 100
        transactions['net_revenue'] = transactions['gross_revenue'] - transactions['discount_amount']
        
        # Generate marketing campaigns
        campaigns = pd.DataFrame({
            'campaign_id': range(1, 51),
            'campaign_name': [f'Campaign {i}' for i in range(1, 51)],
            'start_date': pd.date_range('2022-01-01', '2024-06-01', periods=50),
            'end_date': pd.date_range('2022-02-01', '2024-07-01', periods=50),
            'budget': np.round(np.random.uniform(1000, 50000, 50), 2),
            'channel': np.random.choice(['Google Ads', 'Facebook', 'Email', 'Instagram', 'YouTube'], 50),
            'target_segment': np.random.choice(['Premium', 'Standard', 'Basic', 'All'], 50)
        })
        
        # Store all datasets
        self.datasets.update({
            'customers': customers,
            'products': products,
            'suppliers': suppliers,
            'transactions': transactions,
            'campaigns': campaigns
        })
        
        print("✅ Synthetic data generated successfully!")
        for name, df in self.datasets.items():
            print(f"   📊 {name}: {df.shape}")
        
        return self.datasets
    
    def save_datasets_to_csv(self, folder_path="data"):
        """Save all datasets to CSV files"""
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        for name, df in self.datasets.items():
            file_path = f"{folder_path}/{name}.csv"
            df.to_csv(file_path, index=False)
            print(f"💾 Saved {name} to {file_path}")

# Initialize and run data acquisition
data_acquisition = EcommerceDataAcquisition()
datasets = data_acquisition.download_sample_ecommerce_data()

# Display sample data
print("\n📋 Sample data preview:")
for name, df in datasets.items():
    print(f"\n--- {name.upper()} ---")
    print(df.head(3))
    print(f"Shape: {df.shape}")

main_dataset = datasets["retail"]
