import psycopg2
import csv


csv_file_customers = "north_data/customers_data.csv"
csv_file_employees = "north_data/employees_data.csv"
csv_file_orders = "north_data/orders_data.csv"


# connect to db
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="12345"
)


# create cursor
cur = conn.cursor()


# create functions writing data
def writing_data_to_table(filename, table_name):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            quantity_el = '%s, ' * len(row)
            cur.execute(f"INSERT INTO {table_name} VALUES({quantity_el[:-2]})", (row[::]))


try:
    with conn:
        with conn.cursor() as cur:
            writing_data_to_table(csv_file_customers, 'customers')

            writing_data_to_table(csv_file_employees, 'employees')

            writing_data_to_table(csv_file_orders, 'orders')
finally:
    conn.close()
