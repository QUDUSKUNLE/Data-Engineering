# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import csv

file = open(r'/Users/andeladeveloper/D3/python-projects/Data Engineering/src/dataDrivenInvestor/src/Crypto.csv')
file = csv.reader(file)
print(file)


# %%
# Separate coins by name
litecoin = []
bitcoin = []
ethereum = []
ripple = []
for row in file:
    print(row)
    if row[0] == 'Litecoin':
        litecoin.append(row)
    elif row[0] == 'Bitcoin':
        bitcoin.append(row)
    elif row[0] == 'Ethereum':
        ethereum.append(row)
    elif row[0] == 'Ripple':
        ripple.append(row)

print(len(litecoin), len(bitcoin), len(ethereum), len(ripple))


# %%
# Extract data by
print(litecoin)


# %%
print(len(bitcoin))


# %%
# Write each data into a file
import sqlite3

def create_database(db_name, table):
    conn = sqlite3.connect(db_name)
    
    # Drop table if already exist
    try:
        conn.execute(f'DROP TABLE IF EXISTS {table}')
        print(f'Table {table} dropped successfully')
    except Exception as e:
        print(str(e))
        print(f'Error exists while dropping table {table}')
    
    # Create a new table
    try:
        conn.execute(f'''CREATE TABLE {table}
            (ID     INTEGER PRIMARY KEY,
            Name    TEXT NOT NULL,
            Date    datetime,
            Open    Float default 0,
            High    Float default 0,
            Low     Float default 0,
            Close   float default 0)''' )
        print(f'Table {table} created successfully')
    except Exception as e:
        print(str(e))
        print(f'Error while creating table {table}')
    finally:
        conn.close() # Close database connection


# %%
# Create each coin database and table
bitcoin_database = create_database('bitcoin.db', 'Bitcoin')
litecoin_database = create_database('litecoin.db', 'Litecoin')
ethereum_database = create_database('ethereum.db', 'Ethereum')
ripple_database = create_database('ripple.db', 'Ripple')


# %%
# Insert data into the db
def insert_data(data, db_name, table):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    try:
        cur.executemany(f'insert into {table}(Name, Date, Open, High, Low, Close) Values(?,?,?,?,?,?)', data)
        conn.commit()
        print('Data inserted successfully')
    except Exception as e:
        print(str(e))
        print('Error inserting into the database')
    finally:
        conn.close()

# Insert Data into the database
insert_bitcoin = insert_data(bitcoin, 'bitcoin.db', 'Bitcoin')
insert_litecoin = insert_data(litecoin, 'litecoin.db', 'Litecoin')
insert_ethereum = insert_data(ethereum, 'ethereum.db', 'Ethereum')
insert_ripple = insert_data(ripple, 'ripple.db', 'Ripple')


# %%
# Read data from the db
def read_data_from_database(db_name, table):
    conn = sqlite3.connect(db_name)
    try:
        fetched_data = conn.cursor().execute(f'Select * from {table}')
        print('fetched data successfully')
        for data in fetched_data:
            print(data)
        return fetched_data
    except Exception as e:
        print(str(e))
        print('Error fetching data from the database')
    finally:
        conn.close()


# Fetched data
bitcoin_data_from_database = read_data_from_database('bitcoin.db', 'Bitcoin')
ripple_data_from_dababase = read_data_from_database('ripple.db', 'Ripple')
litecoin_data_from_database = read_data_from_database('litecoin.db', 'Litecoin')
ethereum_data_from_database = read_data_from_database('ethereum.db', 'Ethereum')


# %%
# Write data into csv
def write_data_csv(data, file_name):
    header = ['Name', 'Date', 'Open', 'High', 'Low', 'Close']
    csv_file = open(file_name, 'w')
    csv_writer = csv.writer(csv_file, lineterminator='\r')
    csv_writer.writerow(header)
    csv_writer.writerows(data)
    csv_file.close()
    print(f'Data written to file {file_name} successfully')

# Execute write into csv file
write_data_csv(bitcoin, 'bitcoin.csv') # Write bitcoins into bitcoin.csv
write_data_csv(ripple, 'ripple.csv') # Write ripples into ripple.csv
write_data_csv(litecoin, 'litecoin.csv') # Write litecoins into litecoin.csv
write_data_csv(ethereum, 'ethereum.csv') # Write ethereums into ethereum.csv


# %%
