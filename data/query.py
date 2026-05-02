import duckdb

con = duckdb.connect('data/ecommerce.db', read_only=True)

# Interactive mode
print("DuckDB Query Tool")
print("Type 'exit' to quit\n")

while True:
    query = input("SQL> ")
    
    if query.lower() == 'exit':
        break
    
    if query.strip() == '':
        continue
    
    try:
        result = con.execute(query).df()
        print(result)
        print(f"\n({len(result)} rows)\n")
    except Exception as e:
        print(f"Error: {e}\n")

con.close()