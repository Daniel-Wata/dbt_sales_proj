import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
from google.cloud import bigquery

# Initialize Faker and set random seed for reproducibility
fake = Faker()
random.seed(42)
np.random.seed(42)


from google.oauth2 import service_account
import json


def get_credentials():
    scopes= ["https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/spreadsheets",'https://www.googleapis.com/auth/cloud-platform',            
                                                                                       "https://www.googleapis.com/auth/drive",
                                                                                       "https://www.googleapis.com/auth/bigquery"]
    credentials = service_account.Credentials.from_service_account_file('C:/Users/daniel.watanabe_ingr/Documents/dbt-study/dbt-env/dbt_sales_proj/sa.json')
    return credentials

def generate_client():
    credentials = get_credentials()
    client = bigquery.Client(credentials=credentials)
    return client

client = generate_client()
# Your BigQuery dataset name
dataset_name = 'raw_layer'  # Change if your dataset name is different

# Your GCP project ID
project_id = client.project

# List of possible currencies
currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD']

# Generate Users Table
number_of_users = 100
users = []

for i in range(1, number_of_users + 1):
    user = {
        'id': i,
        'name': fake.name(),
        'email': fake.email(),
        'address': fake.address().replace('\n', ', ')
    }
    users.append(user)

users_df = pd.DataFrame(users)

# Generate Seller Conditions Table
number_of_sellers = 20
sellers = []

for i in range(1, number_of_sellers + 1):
    seller_currency = random.choice(currencies)
    seller = {
        'id': i,
        'seller_name': fake.company(),
        'fee_percentage': round(random.uniform(0.05, 0.20), 4),  # Fee between 5% and 20%
        'currency': seller_currency,
        'days_until_liquidation': random.randint(1, 30)  # Number of days until liquidation
    }
    sellers.append(seller)

sellers_df = pd.DataFrame(sellers)

# Generate Products Table
number_of_products = 200
products = []

for i in range(1, number_of_products + 1):
    seller_id = random.randint(1, number_of_sellers)
    product = {
        'id': i,
        'product_name': fake.word().title(),
        'seller_id': seller_id,
        'price': round(random.uniform(10, 500), 2)
    }
    products.append(product)

products_df = pd.DataFrame(products)

# Generate Orders Table
number_of_orders = 500
orders = []

for i in range(1, number_of_orders + 1):
    order = {
        'id': i,
        'user_id': random.randint(1, number_of_users),
        'order_date': fake.date_between(start_date='-1y', end_date='today'),
        'installments': random.randint(1, 12)  # Installments between 1 and 12
    }
    orders.append(order)

orders_df = pd.DataFrame(orders)

# Generate Order Items Table
order_items = []
order_item_id = 1

for order in orders:
    num_items = random.randint(1, 5)  # Each order has 1 to 5 items
    items = random.sample(range(1, number_of_products + 1), num_items)
    for product_id in items:
        product = products_df[products_df['id'] == product_id].iloc[0]
        seller_id = product['seller_id']
        price = product['price']
        # Get the fee_percentage from seller_conditions
        fee_percentage = sellers_df[sellers_df['id'] == seller_id]['fee_percentage'].iloc[0]
        fee_value = round(price * fee_percentage, 2)
        order_item = {
            'id': order_item_id,
            'order_id': order['id'],
            'product_id': product_id,
            'seller_id': seller_id,
            'price': price,
            'fee_value': fee_value
            # No 'currency' column
        }
        order_items.append(order_item)
        order_item_id += 1

order_items_df = pd.DataFrame(order_items)

# Calculate products_value and fee_value per order
products_value_df = order_items_df.groupby('order_id')['price'].sum().reset_index()
products_value_df.rename(columns={'price': 'products_value'}, inplace=True)

fee_value_df = order_items_df.groupby('order_id')['fee_value'].sum().reset_index()
fee_value_df.rename(columns={'fee_value': 'fee_value'}, inplace=True)

# Merge into orders_df
orders_df = pd.merge(orders_df, products_value_df, left_on='id', right_on='order_id', how='left')
orders_df = pd.merge(orders_df, fee_value_df, left_on='id', right_on='order_id', how='left')

# Function to calculate interest_fee
def calculate_interest_fee(installments, products_value):
    if installments == 1:
        interest_rate = 0
    elif installments <= 3:
        interest_rate = (installments - 1) * 0.01
    elif installments <=6:
        interest_rate = (installments - 1) * 0.015
    else:
        interest_rate = (installments - 1) * 0.02
    interest_fee = round(products_value * interest_rate, 2)
    return interest_fee

# Now calculate interest_fee
orders_df['interest_fee'] = orders_df.apply(lambda row: calculate_interest_fee(row['installments'], row['products_value']), axis=1)

# Calculate total_transaction_value
orders_df['total_transaction_value'] = orders_df['products_value'] + orders_df['fee_value'] + orders_df['interest_fee']

# Drop unnecessary columns
orders_df.drop(columns=['order_id_x', 'order_id_y'], inplace=True, errors='ignore')

# Optional: Reorder Columns for Clarity
orders_df = orders_df[['id', 'user_id', 'order_date', 'installments', 'products_value', 'fee_value', 'interest_fee', 'total_transaction_value']]

# Prepare the Currency Exchange Rates Table
# Get the date range for which we need exchange rates
order_dates = pd.to_datetime(orders_df['order_date'])
start_date = order_dates.min()
end_date = order_dates.max()

date_range = pd.date_range(start=start_date, end=end_date, freq='D')

# Generate mock exchange rates for each currency on each date
exchange_rates_list = []

for single_date in date_range:
    for currency in currencies:
        if currency == 'USD':
            rate = 1.0  # USD is the base currency
        else:
            # Generate a random exchange rate for the currency on that date
            rate = round(random.uniform(0.5, 1.5), 4)
        exchange_rate = {
            'date': single_date.date(),
            'currency': currency,
            'exchange_rate_to_usd': rate
        }
        exchange_rates_list.append(exchange_rate)

currency_exchange_rates_df = pd.DataFrame(exchange_rates_list)

# Function to upload DataFrame to BigQuery
def upload_to_bigquery(df, table_name):
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Convert date columns to strings if necessary
    # BigQuery can handle pandas datetime objects, but ensure they're in correct format
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.date

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()  # Wait for the job to complete

    print(f"Loaded {len(df)} rows into {table_id}.")

# Upload DataFrames to BigQuery
upload_to_bigquery(users_df, 'users')
upload_to_bigquery(sellers_df, 'seller_conditions')
upload_to_bigquery(products_df, 'products')
upload_to_bigquery(orders_df, 'orders')
upload_to_bigquery(order_items_df, 'order_items')
upload_to_bigquery(currency_exchange_rates_df, 'currency_exchange_rates')