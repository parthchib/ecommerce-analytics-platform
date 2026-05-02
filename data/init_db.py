import duckdb

# Create empty DuckDB database
con = duckdb.connect('data/ecommerce.db')

print("✅ Created ecommerce.db")
print(f"📍 Location: data/ecommerce.db")

con.close()