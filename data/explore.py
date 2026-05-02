import pandas as pd
import os

RAW_DATA_DIR = 'raw_data'

print("="*60)
print("🔍 DATA EXPLORATION")
print("="*60)

files = {
    'orders': 'orders.csv',
    'products': 'products.csv', 
    'order_products': 'order_products__prior.csv',
    'aisles': 'aisles.csv',
    'departments': 'departments.csv'
}

for name, filename in files.items():
    print(f"\n📊 {name.upper()}")
    filepath = os.path.join(RAW_DATA_DIR, filename)
    
    # Read sample
    df = pd.read_csv(filepath, nrows=20000)
    
    print(f"  Rows (sample): {len(df):,}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Nulls: {df.isnull().sum().to_dict()}")
    print(f"  Duplicates: {df.duplicated().sum()}")

print("\n✅ Exploration complete!")