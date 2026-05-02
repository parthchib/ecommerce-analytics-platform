import pandas as pd
import duckdb
import os
from datetime import datetime

RAW_DATA_DIR = 'raw_data'
DB_PATH = 'data/ecommerce.db'

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

print("="*60)
print("🧹 DATA CLEANING & LOADING")
print("="*60)

# ============================================================
# 1. LOAD DIMENSION TABLES (small, no chunking needed)
# ============================================================

print("\n📦 Loading dimension tables...")

# Aisles
aisles = pd.read_csv(os.path.join(RAW_DATA_DIR, 'aisles.csv'))
print(f"  ✓ Aisles: {len(aisles):,} rows")

# Departments
departments = pd.read_csv(os.path.join(RAW_DATA_DIR, 'departments.csv'))
print(f"  ✓ Departments: {len(departments):,} rows")

# Products (enrich with aisle & department names)
products = pd.read_csv(os.path.join(RAW_DATA_DIR, 'products.csv'))
products = products.merge(aisles, on='aisle_id', how='left')
products = products.merge(departments, on='department_id', how='left')
print(f"  ✓ Products: {len(products):,} rows (enriched)")

# Save to DuckDB
con.execute("CREATE TABLE IF NOT EXISTS stg_aisles AS SELECT * FROM aisles")
con.execute("CREATE TABLE IF NOT EXISTS stg_departments AS SELECT * FROM departments")
con.execute("CREATE TABLE IF NOT EXISTS stg_products AS SELECT * FROM products")

# ============================================================
# 2. CLEAN & LOAD ORDERS (large, use chunking)
# ============================================================

print("\n📦 Cleaning & loading orders (chunked)...")

orders_file = os.path.join(RAW_DATA_DIR, 'orders.csv')
chunk_size = 100000
total_rows = 0

# Drop table if exists
con.execute("DROP TABLE IF EXISTS stg_orders")

for i, chunk in enumerate(pd.read_csv(orders_file, chunksize=chunk_size)):
    # CLEANING LOGIC
    
    # 1. Fill null days_since_prior_order with 0
    chunk['days_since_prior_order'] = chunk['days_since_prior_order'].fillna(0)
    
    # 2. Add is_first_order flag
    chunk['is_first_order'] = (chunk['days_since_prior_order'] == 0).astype(int)
    
    # 3. Ensure correct data types
    chunk['order_id'] = chunk['order_id'].astype(int)
    chunk['user_id'] = chunk['user_id'].astype(int)
    chunk['order_number'] = chunk['order_number'].astype(int)
    chunk['days_since_prior_order'] = chunk['days_since_prior_order'].astype(float)
    
    # 4. Add load timestamp
    chunk['loaded_at'] = datetime.now()
    
    # Insert into DuckDB
    if i == 0:
        # First chunk: create table
        con.execute("CREATE TABLE stg_orders AS SELECT * FROM chunk")
    else:
        # Subsequent chunks: append
        con.execute("INSERT INTO stg_orders SELECT * FROM chunk")
    
    total_rows += len(chunk)
    
    if (i + 1) % 10 == 0:
        print(f"  Processed {total_rows:,} rows...")

print(f"  ✓ Orders: {total_rows:,} rows loaded")

# ============================================================
# 3. CLEAN & LOAD ORDER_PRODUCTS (very large, use chunking)
# ============================================================

print("\n📦 Cleaning & loading order products (chunked)...")

order_products_file = os.path.join(RAW_DATA_DIR, 'order_products__prior.csv')
total_rows = 0

# Drop table if exists
con.execute("DROP TABLE IF EXISTS stg_order_products")

# Get valid IDs for validation
valid_order_ids = set(con.execute("SELECT order_id FROM stg_orders").fetchdf()['order_id'])
valid_product_ids = set(con.execute("SELECT product_id FROM stg_products").fetchdf()['product_id'])

print(f"  Valid order_ids: {len(valid_order_ids):,}")
print(f"  Valid product_ids: {len(valid_product_ids):,}")

for i, chunk in enumerate(pd.read_csv(order_products_file, chunksize=chunk_size)):
    # CLEANING LOGIC
    
    # 1. Validate foreign keys (remove orphaned records)
    before = len(chunk)
    chunk = chunk[chunk['order_id'].isin(valid_order_ids)]
    chunk = chunk[chunk['product_id'].isin(valid_product_ids)]
    after = len(chunk)
    
    if before != after:
        print(f"  ⚠️  Removed {before - after} invalid records")
    
    # 2. Ensure correct data types
    chunk['order_id'] = chunk['order_id'].astype(int)
    chunk['product_id'] = chunk['product_id'].astype(int)
    chunk['add_to_cart_order'] = chunk['add_to_cart_order'].astype(int)
    chunk['reordered'] = chunk['reordered'].astype(int)
    
    # 3. Add load timestamp
    chunk['loaded_at'] = datetime.now()
    
    # Insert into DuckDB
    if i == 0:
        con.execute("CREATE TABLE stg_order_products AS SELECT * FROM chunk")
    else:
        con.execute("INSERT INTO stg_order_products SELECT * FROM chunk")
    
    total_rows += len(chunk)
    
    if (i + 1) % 50 == 0:
        print(f"  Processed {total_rows:,} rows...")

print(f"  ✓ Order products: {total_rows:,} rows loaded")

# ============================================================
# 4. VALIDATION
# ============================================================

print("\n" + "="*60)
print("✅ VALIDATION SUMMARY")
print("="*60)

tables = ['stg_aisles', 'stg_departments', 'stg_products', 'stg_orders', 'stg_order_products']

for table in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count:,} rows")

# Check for nulls in critical columns
print("\n🔍 Null Check:")
null_check = con.execute("""
    SELECT 
        COUNT(*) as total_orders,
        SUM(CASE WHEN days_since_prior_order IS NULL THEN 1 ELSE 0 END) as null_days
    FROM stg_orders
""").fetchdf()
print(null_check.to_string(index=False))

print("\n" + "="*60)
print("✅ CLEANING & LOADING COMPLETE!")
print("="*60)

con.close()