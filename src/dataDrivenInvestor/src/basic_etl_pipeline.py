# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import csv

f = open(r'/Users/andeladeveloper/D3/python-projects/Data Engineering/src/dataDrivenInvestor/src/crypto-markets.csv')
csv_reader = csv.reader(f)
print(csv_reader)


# %%
assetsCode = ['BTC', 'ETH', 'XRP', 'LTC']
# initialize empty data

next(csv_reader, None)

crypto_data = []

for row in csv_reader:
    print(row)
    if (row[1] in assetsCode):
        row[5] = float(row[5]) * 0.75
        row[6] = float(row[6]) * 0.75
        row[7] = float(row[7]) * 0.75
        row[8] = float(row[8]) * 0.75
        crypto_data.append(row)
print(dir(csv_reader))
print(len(crypto_data))
print(crypto_data[0:2])


# %%
import sqlite3
conn = sqlite3.connect('session.db')

# Drop table if already exists
try:
    conn.execute('DROP TABLE IF EXISTS `Crypto`')
    print('DROP TABLE `Crypto`')
except Exception as e:
    print(str(e))

# Create a new table
try:
    conn.execute('''
        CREATE TABLE Crypto
        (ID        INTEGER PRIMARY KEY,
        Name       TEXT NOT NULL,
        Date       datetime,
        Open       Float DEFAULT 0,
        High       Float DEFAULT 0,
        Low        Float DEFAULT 0,
        Close      Float DEFAULT 0);''')
    print('Table created successfully')
except Exception as e:
    print(str(e))
    print('Table creation failed!!!')
finally:
    conn.close() # Close database connection


# %%
for row in csv_reader:
    print(row)


# %%
print(csv_reader)


# %%
print(csv_reader.line_num)


# %%
print(crypto_data[0])


# %%
crypto_sql_data = [(row[2], row[3], row[5], row[6], row[7], row[8]) for row in crypto_data]
crypto_sql_data[:2]


# %%
conn = sqlite3.connect('session.db')
cur = conn.cursor()
try:
    cur.executemany("INSERT INTO Crypto(Name, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)", crypto_sql_data)
    conn.commit()
    print('Data inserted successfully')
except Exception as e:
    print(str(e))
    print('Data insertion failed')
finally:
    conn.close()


# %%
# Reading Data from the database
conn = sqlite3.connect('session.db')
rows = conn.cursor().execute('SELECT * from Crypto')

for row in rows:
    print(row)


# %%
## Write data in a csv files
csv_file = open('extract_data.csv', 'w')
csv_writer = csv.writer(csv_file, lineterminator='\r')
csv_writer.writerow(['Name', 'Date', 'Open', 'High', 'Low', 'Close'] )
csv_writer.writerows(crypto_sql_data)
csv_file.close()


# Simple ETL simply mean extract, transform and loading of data for analysis.
# By doing this, information are been derived from these data to make business decisions or product decisions

